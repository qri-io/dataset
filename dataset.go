// Package dataset contains the qri ("query") dataset document definition
// This package contains the base definition, as well as a number of
// subpackages that build from this base to add functionality as necessary
// Datasets take inspiration from HTML documents, deliniating semantic purpose
// to predefined tags of the document, but instead of orienting around
// presentational markup, dataset documents emphasize interoperability and
// composition. The principle encoding format for a dataset document is JSON.
//
// Alpha-Keys:
// Dataset documents are designed to produce consistent checksums when encoded
// for storage & transmission. To keep hashing consistent map keys are sorted
// lexographically for encoding. This applies to all fields of a dataset
// document except the body of a dataaset, where users may need to dictate the
// ordering of map keys
//
// Pod ("Plain old Data") Pattern:
// To maintain high interoperability, dataset documents must support encoding &
// decoding ("coding", or "serialization") to and from many formats, fields of
// dataset documents that leverage "exotic" custom types are acommpanied by a
// "Plain Old Data" variant, denoted by a "Pod" suffix in their name
// Plain-Old-Data variants use only basic go types:
// string, bool, int, float64, []interface{}, etc.
// and have methods for clean encoding and decoding to their exotic forms
package dataset

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	logger "github.com/ipfs/go-log"
)

// log is the internal logging mechanism for the dataset package
var log = logger.Logger("dataset")

// Dataset is a document for describing & storing structured data.
// Dataset documents are designed to satisfy the FAIR principle of being
// Findable, Accessible, Interoperable, and Reproducible, in relation to other
// dataset documents, and related-but-separate technologies such as data
// catalogs, HTTP API's, and data package formats
// Datasets are designed to be stored and distributed on content-addressed
// (identify-by-hash) systems
// The dataset document definition is built from a research-first principle,
// valuing direct interoperability with existing standards over novel
// definitions or specifications
type Dataset struct {
	// private storage for reference to this object
	path datastore.Key

	// Commit contains author & change message information that describes this
	// version of a dataset
	Commit *Commit `json:"commit,omitempty"`
	// BodyPath is the path to the hash of raw data as it resolves on the network
	// Datasets have at most one body
	BodyPath string `json:"bodyPath,omitempty"`
	// Meta contains all human-readable meta about this dataset intended to aid
	// in discovery and organization of this document
	Meta *Meta `json:"meta,omitempty"`
	// PreviousPath connects datasets to form a historical merkle-DAG of snapshots
	// of this document, creating a version history
	PreviousPath string `json:"previousPath,omitempty"`
	// Qri is a key for both identifying this document type, and versioning the
	// dataset document definition itself.
	Qri Kind `json:"qri"`
	// Structure of this dataset
	Structure *Structure `json:"structure"`
	// Transform is a path to the transformation that generated this resource
	Transform *Transform `json:"transform,omitempty"`
	// VisConfig stores configuration data related to representing a dataset as
	// a visualization
	VisConfig *VisConfig `json:"visconfig,omitempty"`
}

// IsEmpty checks to see if dataset has any fields other than the internal path
func (ds *Dataset) IsEmpty() bool {
	return ds.Commit == nil &&
		ds.Structure == nil &&
		ds.BodyPath == "" &&
		ds.Meta == nil &&
		ds.PreviousPath == "" &&
		ds.Transform == nil &&
		ds.VisConfig == nil
}

// Path gives the internal path reference for this dataset
func (ds *Dataset) Path() datastore.Key {
	return ds.path
}

// NewDatasetRef creates a Dataset pointer with the internal
// path property specified, and no other fields.
func NewDatasetRef(path datastore.Key) *Dataset {
	return &Dataset{path: path}
}

// Abstract returns a copy of dataset with all
// semantically-identifiable and concrete references replaced with
// uniform values
func Abstract(ds *Dataset) *Dataset {
	abs := &Dataset{Qri: ds.Qri}
	if ds.Structure != nil {
		abs.Structure = &Structure{}
		abs.Structure.Assign(ds.Structure.Abstract())
	}
	return abs
}

// SetPath sets the internal path property of a dataset
// Use with caution. most callers should never need to call SetPath
func (ds *Dataset) SetPath(path string) {
	if path == "" {
		ds.path = datastore.Key{}
	} else {
		ds.path = datastore.NewKey(path)
	}
}

// SignableBytes produces the portion of a commit message used for signing
// the format for signable bytes is:
// *  commit timestamp in RFC3339 format, UTC timezone
// *  newline character
// *  dataset structure checksum string
// checksum string should be a base58-encoded multihash of the dataset data
func (ds *Dataset) SignableBytes() ([]byte, error) {
	if ds.Commit == nil {
		return nil, fmt.Errorf("commit is required")
	}
	if ds.Structure == nil {
		return nil, fmt.Errorf("structure is required")
	}
	return []byte(fmt.Sprintf("%s\n%s", ds.Commit.Timestamp.UTC().Format(time.RFC3339), ds.Structure.Checksum)), nil
}

// Assign collapses all properties of a group of datasets onto one.
// this is directly inspired by Javascript's Object.assign
func (ds *Dataset) Assign(datasets ...*Dataset) {
	for _, d := range datasets {
		if d == nil {
			continue
		}

		if d.path.String() != "" {
			ds.path = d.path
		}
		if ds.Structure == nil && d.Structure != nil {
			ds.Structure = d.Structure
		} else if ds.Structure != nil {
			ds.Structure.Assign(d.Structure)
		}
		if ds.Meta == nil && d.Meta != nil {
			ds.Meta = d.Meta
		} else if ds.Meta != nil {
			ds.Meta.Assign(d.Meta)
		}
		if ds.Transform == nil && d.Transform != nil {
			ds.Transform = d.Transform
		} else if ds.Transform != nil {
			ds.Transform.Assign(d.Transform)
		}
		if ds.Commit == nil && d.Commit != nil {
			ds.Commit = d.Commit
		} else if ds.Commit != nil {
			ds.Commit.Assign(d.Commit)
		}
		if ds.VisConfig == nil && d.VisConfig != nil {
			ds.VisConfig = d.VisConfig
		} else if ds.VisConfig != nil {
			ds.VisConfig.Assign(d.VisConfig)
		}

		if d.BodyPath != "" {
			ds.BodyPath = d.BodyPath
		}
		if d.PreviousPath != "" {
			ds.PreviousPath = d.PreviousPath
		}
		// TODO - wut dis?
		ds.Commit.Assign(d.Commit)
	}
}

// MarshalJSON uses a map to combine meta & standard fields.
// Marshalling a map[string]interface{} automatically alpha-sorts the keys.
func (ds *Dataset) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal to a string instead
	// TODO - check all fields
	if ds.path.String() != "" && ds.IsEmpty() {
		return ds.path.MarshalJSON()
	}
	if ds.Qri == "" {
		ds.Qri = KindDataset
	}

	return json.Marshal(_dataset(*ds))
}

// internal struct for json unmarshaling
type _dataset Dataset

// UnmarshalJSON implements json.Unmarshaller
func (ds *Dataset) UnmarshalJSON(data []byte) error {
	// first check to see if this is a valid path ref
	var path string
	if err := json.Unmarshal(data, &path); err == nil {
		*ds = Dataset{path: datastore.NewKey(path)}
		return nil
	}

	d := _dataset{}
	if err := json.Unmarshal(data, &d); err != nil {
		log.Debug(err.Error())
		return fmt.Errorf("unmarshaling dataset: %s", err.Error())
	}
	*ds = Dataset(d)
	return nil
}

// UnmarshalDataset tries to extract a dataset type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalDataset(v interface{}) (*Dataset, error) {
	switch r := v.(type) {
	case *Dataset:
		return r, nil
	case Dataset:
		return &r, nil
	case []byte:
		dataset := &Dataset{}
		err := json.Unmarshal(r, dataset)
		return dataset, err
	default:
		err := fmt.Errorf("couldn't parse dataset, value is invalid type")
		log.Debug(err.Error())
		return nil, err
	}
}

// Encode creates a DatasetPod from a Dataset instance
func (ds Dataset) Encode() *DatasetPod {
	cd := &DatasetPod{
		BodyPath:     ds.BodyPath,
		Meta:         ds.Meta,
		Path:         ds.Path().String(),
		PreviousPath: ds.PreviousPath,
		Qri:          ds.Qri.String(),
		VisConfig:    ds.VisConfig,
	}

	if ds.Commit != nil {
		cd.Commit = ds.Commit.Encode()
	}
	if ds.Structure != nil {
		cd.Structure = ds.Structure.Encode()
	}
	if ds.Transform != nil {
		cd.Transform = ds.Transform.Encode()
	}

	return cd
}

// Decode creates a Dataset from a DatasetPod instance
func (ds *Dataset) Decode(cd *DatasetPod) error {
	d := Dataset{
		path:         datastore.NewKey(cd.Path),
		BodyPath:     cd.BodyPath,
		PreviousPath: cd.PreviousPath,
		Meta:         cd.Meta,
		VisConfig:    cd.VisConfig,
	}

	if cd.Qri != "" {
		// TODO - this should react to changes in cd
		d.Qri = KindDataset
	}

	if cd.Commit != nil {
		d.Commit = &Commit{}
		if err := d.Commit.Decode(cd.Commit); err != nil {
			return err
		}
	}

	if cd.Structure != nil {
		d.Structure = &Structure{}
		if err := d.Structure.Decode(cd.Structure); err != nil {
			return err
		}
	}

	if cd.Transform != nil {
		d.Transform = &Transform{}
		if err := d.Transform.Decode(cd.Transform); err != nil {
			return err
		}
	}

	*ds = d
	return nil
}

// DatasetPod is a variant of Dataset safe for encoding & decoding to static
// formats, using only simple go types
// DatasetPod can contain values that only exist after a dataset has been stored
// in a content-addressed system, such as path, and fields that implicitly on
// dataset having a path, like Peername & Name
// There are also two fields that may contain dataset data: Data & DataBytes.
// In practice these are only populated in special situations, and often only
// one of the two Data fields is populated at a time.
type DatasetPod struct {
	Commit *CommitPod `json:"commit,omitempty"`
	// Body is the designated field for representing dataset data with native go
	// types. this will often not be populated
	Body interface{} `json:"body,omitempty"`
	// BodyBytes is fpr representing dataset data as a slice of bytes
	// this will often not be populated
	BodyBytes []byte `json:"bodyBytes,omitempty"`
	// BodyPath is the path to retrieve this dataset
	BodyPath string `json:"bodyPath,omitempty"`
	// Unique name reference for this dataset
	Name string `json:"name,omitempty"`
	Meta *Meta  `json:"meta,omitempty"`
	Path string `json:"path,omitempty"`
	// Peername of dataset owner
	Peername     string `json:"peername,omitempty"`
	PreviousPath string `json:"previousPath,omitempty"`
	// ProfileID of dataset owner
	ProfileID string        `json:"profileID,omitempty"`
	Qri       string        `json:"qri"`
	Structure *StructurePod `json:"structure"`
	Transform *TransformPod `json:"transform,omitempty"`
	VisConfig *VisConfig    `json:"visconfig,omitempty"`
}
