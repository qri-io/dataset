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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/qri-io/qfs"
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
	// body file reader, doesn't serialize
	bodyFile qfs.File
	// Body represents dataset data with native go types.
	// Datasets have at most one body. Body, BodyBytes, and BodyPath
	// work together, often with only one field used at a time
	Body interface{} `json:"body,omitempty"`
	// BodyBytes is for representing dataset data as a slice of bytes
	BodyBytes []byte `json:"bodyBytes,omitempty"`
	// BodyPath is the path to the hash of raw data as it resolves on the network
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
	// Readme is a path to the readme file for this dataset
	Readme *Readme `json:"readme,omitempty"`
	// Number of versions this dataset has, transient
	NumVersions int `json:"numVersions,omitempty"`
	// Qri is a key for both identifying this document type, and versioning the
	// dataset document definition itself. derived
	Qri string `json:"qri"`
	// Structure of this dataset
	Structure *Structure `json:"structure,omitempty"`
	// Stats is a component containing statistical metadata about the dataset body
	Stats *Stats `json:"stats,omitempty"`
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
		ds.Readme == nil &&
		ds.Stats == nil &&
		ds.Viz == nil
}

// NewDatasetRef creates a Dataset pointer with the internal
// path property specified, and no other fields.
func NewDatasetRef(path string) *Dataset {
	return &Dataset{Path: path}
}

// SigningBytes produces a set of bytes for signing to establish authorship of a
// dataset. The signing bytes is a newline-delimited, alpha-sorted list of
// components within the dataset, where each component is identified by a two
// letter prefix and a colon ':' character:
//
//   two_letter_component_type ':' component_value
//
// the component value for all components except commit is the path of the
// component. For the commit component, the value is the value of
// commit.Timestamp in nanosecond-RFC3339 format, UTC timezone
//
// When used in conjunction with a merkelizd filesystem path values are also
// content checksums. A signature of SigningBytes on a merkelized filesystem
// affirms time, author, and contents
// When used in with a mutable filesystem, SigningBytes is a weaker claim that
// only affirms time, author, and path values
func (ds *Dataset) SigningBytes() []byte {
	var sigComponents []string

	if ds.BodyPath != "" {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindBody, ds.BodyPath))
	}
	if ds.Commit != nil && !ds.Commit.Timestamp.IsZero() {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindCommit, ds.Commit.Timestamp.UTC().Format(time.RFC3339)))
	}
	if ds.Meta != nil && ds.Meta.Path != "" {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindMeta, ds.Meta.Path))
	}
	if ds.Readme != nil && ds.Readme.Path != "" {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindReadme, ds.Readme.Path))
	}
	if ds.Structure != nil && ds.Structure.Path != "" {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindStructure, ds.Structure.Path))
	}
	if ds.Transform != nil && ds.Transform.Path != "" {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindTransform, ds.Transform.Path))
	}
	if ds.Stats != nil && ds.Stats.Path != "" {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindStats, ds.Stats.Path))
	}
	if ds.Viz != nil && ds.Viz.Path != "" {
		sigComponents = append(sigComponents, ComponentTypePrefix(KindViz, ds.Viz.Path))
	}

	return []byte(strings.Join(sigComponents, "\n"))
}

// SignableBytes produces the portion of a commit message used for signing
// the format for signable bytes is:
// *  commit timestamp in nanosecond-RFC3339 format, UTC timezone
// *  newline character
// *  dataset structure checksum string
// checksum string should be a base58-encoded multihash of the dataset data
// DEPRECATED - use SigningBytes instead
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
// note that DropTransientValues does *not* drop the transient values of child
// components of a dataset, each component's DropTransientValues method must be
// called separately
func (ds *Dataset) DropTransientValues() {
	ds.Body = nil
	ds.BodyBytes = nil
	ds.Name = ""
	ds.Path = ""
	ds.ProfileID = ""
	ds.NumVersions = 0
}

// DropDerivedValues resets all set-on-save fields to their default values
func (ds *Dataset) DropDerivedValues() {
	ds.Qri = ""
	ds.Path = ""

	if ds.Commit != nil {
		ds.Commit.DropDerivedValues()
	}
	if ds.Meta != nil {
		ds.Meta.DropDerivedValues()
	}
	if ds.Structure != nil {
		ds.Structure.DropDerivedValues()
	}
	if ds.Transform != nil {
		ds.Transform.DropDerivedValues()
	}
	if ds.Readme != nil {
		ds.Readme.DropDerivedValues()
	}
	if ds.Stats != nil {
		ds.Stats.DropDerivedValues()
	}
	if ds.Viz != nil {
		ds.Viz.DropDerivedValues()
	}
}

var (
	// ErrInlineBody is the error for attempting to generate a body file when
	// body data is stored as native go types
	ErrInlineBody = fmt.Errorf("dataset body is inlined")
	// ErrNoResolver is an error for missing-but-needed resolvers
	ErrNoResolver = fmt.Errorf("no resolver available to fetch path")
)

// OpenBodyFile sets the byte stream of file data, prioritizing:
// * erroring when the body is inline
// * creating an in-place file from bytes
// * passing BodyPath to the resolver
// once resolved, the file is set to an internal field, which is
// accessible via the BodyFile method. separating into two steps
// decouples loading from access
func (ds *Dataset) OpenBodyFile(ctx context.Context, resolver qfs.PathResolver) (err error) {
	if ds.Body != nil {
		// TODO (b5): this needs thought. Ideally we'd be able to delay
		// decoding of inline data to present a stream of bytes here but that would
		// require acrobatics like preserving the format the dataset itself was decoded from.
		// first glance would be to include an opt-in interface on qfs.File that carries
		// a data format field, have an "OpenDataset" func that reads & decodes from a
		// byte stream, have that method set the internal type, or maybe use that
		// same method to redirect Body to BodyBytes. Either way this feels like it
		// violates our plain-old-data pattern.
		// another option: always use the same data format. CBOR? encoding/gob?
		// option 3: require structure match dataset encoding format for this
		// exact reason. infor dataset data format to match when missing
		return ErrInlineBody
	}

	if ds.BodyBytes != nil {
		bodyPath := ds.BodyPath
		if bodyPath == "" {
			bodyPath = "body"
		}
		ds.bodyFile = qfs.NewMemfileBytes(bodyPath, ds.BodyBytes)
		return nil
	}

	if ds.BodyPath == "" {
		// nothing to resolve
		return nil
	}

	if resolver == nil {
		return ErrNoResolver
	}

	ds.bodyFile, err = resolver.Get(ctx, ds.BodyPath)
	if err != nil {
		return fmt.Errorf("opening dataset.bodyPath '%s': %s", ds.BodyPath, err)
	}
	return
}

// SetBodyFile assigns the bodyFile.
func (ds *Dataset) SetBodyFile(file qfs.File) {
	ds.bodyFile = file
}

// BodyFile exposes bodyFile if one is set. Callers that use the file in any
// way (eg. by calling Read) should consume the entire file and call Close
func (ds *Dataset) BodyFile() qfs.File {
	return ds.bodyFile
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
		if d.bodyFile != nil {
			ds.bodyFile = d.bodyFile
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
		if ds.Stats == nil && d.Stats != nil {
			ds.Stats = d.Stats
		} else if ds.Stats != nil {
			ds.Stats.Assign(d.Stats)
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
		if ds.Readme == nil && d.Readme != nil {
			ds.Readme = d.Readme
		} else if ds.Readme != nil {
			ds.Readme.Assign(d.Readme)
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
