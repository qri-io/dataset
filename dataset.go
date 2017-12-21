package dataset

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-datastore"
)

// Dataset is a description of a single structured data resource. with the following properties:
// * A Dataset must resolve to one and only one entity, specified by a `data` property.
// * All datasets have a structure that defines how to intepret the data.
// * Datasets contain descriptive metadata
// * Though software Dataset metadata is interoperable with the DCAT, Project Open Data,
//   Open Knowledge Foundation DataPackage and JSON-LD specifications,
//   with the one major exception that content-addressed hashes are acceptable in place of urls.
// * Datasets have a "PreviousPath" field that forms historical DAGs
// * Datasets contain a "commit" object that describes changes over time
// * Dataset Commits can and should be author attributed via keypair signing
// * Datasets "Transformations" provide determinstic records of the process used to
//   create a dataset
// * Dataset Structures & Transformations can have Abstract variants
//   that describe a general form of their applicability to other datasets
// Finally, commit messages should also be able to interoperate with git commits
type Dataset struct {
	// private storage for reference to this object
	path datastore.Key

	// Abstract is the abstract form of this dataset
	Abstract *Dataset `json:"abstract,omitempty"`
	// AbstractTransform is a reference to the general form of the transformation
	// that resulted in this dataset
	AbstractTransform *Transform `json:"abstractTransform,omitempty"`
	// Commit contains author & change message information
	Commit *Commit `json:"commit,omitempty"`
	// DataPath is the path to the hash of raw data as it resolves on the network.
	DataPath string `json:"dataPath,omitempty"`
	// Kind is required, must be qri:ds:[version]
	Kind Kind `json:"kind"`
	// Meta contains all human-readable metadata about this dataset
	Metadata *Metadata `json:"metadata,omitempty"`
	// PreviousPath connects datasets to form a historical DAG
	PreviousPath string `json:"previousPath,omitempty"`
	// Structure of this dataset
	Structure *Structure `json:"structure"`
	// Transform is a path to the transformation that generated this resource
	Transform *Transform `json:"transform,omitempty"`
}

// IsEmpty checks to see if dataset has any fields other than the internal path
func (ds *Dataset) IsEmpty() bool {
	return ds.Abstract == nil &&
		ds.AbstractTransform == nil &&
		ds.Commit == nil &&
		ds.DataPath == "" &&
		ds.Metadata == nil &&
		ds.PreviousPath == "" &&
		ds.Transform == nil
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
	abs := &Dataset{Kind: ds.Kind}
	if ds.Structure != nil {
		abs.Structure = &Structure{}
		abs.Structure.Assign(ds.Structure.Abstract())
	}
	return abs
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

		if ds.Abstract == nil && d.Abstract != nil {
			ds.Abstract = d.Abstract
		} else if ds.Abstract != nil {
			ds.Abstract.Assign(d.Abstract)
		}

		if ds.Transform == nil && d.Transform != nil {
			ds.Transform = d.Transform
		} else if ds.Transform != nil {
			ds.Transform.Assign(d.Transform)
		}

		if ds.AbstractTransform == nil && d.AbstractTransform != nil {
			ds.AbstractTransform = d.AbstractTransform
		} else if ds.AbstractTransform != nil {
			ds.AbstractTransform.Assign(d.AbstractTransform)
		}

		if ds.Commit == nil && d.Commit != nil {
			ds.Commit = d.Commit
		} else if ds.Commit != nil {
			ds.Commit.Assign(d.Commit)
		}

		if d.DataPath != "" {
			ds.DataPath = d.DataPath
		}
		if d.PreviousPath != "" {
			ds.PreviousPath = d.PreviousPath
		}
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
	if ds.Kind == "" {
		ds.Kind = KindDataset
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

	// TODO - I'm guessing what follows could be better
	d := _dataset{}
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("error unmarshling dataset: %s", err.Error())
	}

	*ds = Dataset{
		Abstract:          d.Abstract,
		AbstractTransform: d.AbstractTransform,
		Commit:            d.Commit,
		DataPath:          d.DataPath,
		Kind:              d.Kind,
		Metadata:          d.Metadata,
		PreviousPath:      d.PreviousPath,
		Structure:         d.Structure,
		Transform:         d.Transform,
	}
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
		return nil, fmt.Errorf("couldn't parse dataset, value is invalid type")
	}
}
