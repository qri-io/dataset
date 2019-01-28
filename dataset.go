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
)

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
	// Body is the designated field for representing dataset data with native go
	// types. this will often not be populated, transient
	Body interface{} `json:"body,omitempty"`
	// BodyBytes is for representing dataset data as a slice of bytes
	// this will often not be populated, transient
	BodyBytes []byte `json:"bodyBytes,omitempty"`
	// BodyPath is the path to the hash of raw data as it resolves on the network
	// Datasets have at most one body
	BodyPath string `json:"bodyPath,omitempty"`

	// Commit contains author & change message information that describes this
	// version of a dataset
	Commit *Commit `json:"commit,omitempty"`
	// Meta contains all human-readable meta about this dataset intended to aid
	// in discovery and organization of this document
	Meta *Meta `json:"meta,omitempty"`

	// name reference for this dataset, transient
	Name string `json:"name,omitempty"`
	// Location of this dataset, transient
	Path string `json:"path,omitempty"`
	// Peername of dataset owner, transient
	Peername string `json:"peername,omitempty"`
	// PreviousPath connects datasets to form a historical merkle-DAG of snapshots
	// of this document, creating a version history
	PreviousPath string `json:"previousPath,omitempty"`
	// ProfileID of dataset owner, transient
	ProfileID string `json:"profileID,omitempty"`
	// Qri is a key for both identifying this document type, and versioning the
	// dataset document definition itself.
	Qri string `json:"qri"`
	// Structure of this dataset
	Structure *Structure `json:"structure"`
	// Transform is a path to the transformation that generated this resource
	Transform *Transform `json:"transform,omitempty"`
	// Viz stores configuration data related to representing a dataset as
	// a visualization
	Viz *Viz `json:"viz,omitempty"`
}

// IsEmpty checks to see if dataset has any fields other than the Path & Qri fields
func (ds *Dataset) IsEmpty() bool {
	return ds.Body == nil &&
		ds.BodyBytes == nil &&
		ds.BodyPath == "" &&
		ds.Commit == nil &&
		ds.Meta == nil &&
		ds.Name == "" &&
		ds.Peername == "" &&
		ds.PreviousPath == "" &&
		ds.ProfileID == "" &&
		ds.Structure == nil &&
		ds.Transform == nil &&
		ds.Viz == nil
}

// NewDatasetRef creates a Dataset pointer with the internal
// path property specified, and no other fields.
func NewDatasetRef(path string) *Dataset {
	return &Dataset{Path: path}
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

// DropTransientValues removes values that cannot be recorded when the
// dataset is rendered immutable, usually by storing it in a cafs
func (ds *Dataset) DropTransientValues() {
	ds.Body = nil
	ds.BodyBytes = nil
	// ds.Commit.DropTransientValues()
	// ds.Meta.DropTransientValues()
	ds.Name = ""
	ds.Path = ""
	ds.ProfileID = ""
	// ds.Structure.DropTransientValues()
	// ds.Transform.DropTransientValues()
	// ds.Viz.DropTransientValues()
}

// Assign collapses all properties of a group of datasets onto one.
// this is directly inspired by Javascript's Object.assign
func (ds *Dataset) Assign(datasets ...*Dataset) {
	for _, d := range datasets {
		if d == nil {
			continue
		}

		// transient values
		if d.Body != nil {
			ds.Body = d.Body
		}
		if d.BodyBytes != nil {
			ds.BodyBytes = d.BodyBytes
		}
		if d.BodyPath != "" {
			ds.BodyPath = d.BodyPath
		}

		if ds.Commit == nil && d.Commit != nil {
			ds.Commit = d.Commit
		} else if ds.Commit != nil {
			ds.Commit.Assign(d.Commit)
		}
		if ds.Meta == nil && d.Meta != nil {
			ds.Meta = d.Meta
		} else if ds.Meta != nil {
			ds.Meta.Assign(d.Meta)
		}
		if d.Name != "" {
			ds.Name = d.Name
		}
		if d.Path != "" {
			ds.Path = d.Path
		}
		if d.Peername != "" {
			ds.Peername = d.Peername
		}
		if d.PreviousPath != "" {
			ds.PreviousPath = d.PreviousPath
		}
		if d.ProfileID != "" {
			ds.ProfileID = d.ProfileID
		}

		if ds.Structure == nil && d.Structure != nil {
			ds.Structure = d.Structure
		} else if ds.Structure != nil {
			ds.Structure.Assign(d.Structure)
		}
		if ds.Transform == nil && d.Transform != nil {
			ds.Transform = d.Transform
		} else if ds.Transform != nil {
			ds.Transform.Assign(d.Transform)
		}
		if ds.Viz == nil && d.Viz != nil {
			ds.Viz = d.Viz
		} else if ds.Viz != nil {
			ds.Viz.Assign(d.Viz)
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
	if ds.Path != "" && ds.IsEmpty() {
		return json.Marshal(ds.Path)
	}
	if ds.Qri == "" {
		ds.Qri = KindDataset.String()
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
		*ds = Dataset{Path: path}
		return nil
	}

	d := _dataset{}
	if err := json.Unmarshal(data, &d); err != nil {
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
		return nil, err
	}
}
