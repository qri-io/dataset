package dataset

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
)

// Dataset is stored separately from prescriptive metadata stored in Resource structs
// to maximize overlap of the formal transform & resource definitions.
// A Dataset must resolve to one and only one entity, specified by a `data` property.
// It's structure must be specified by a structure definition.
// This also creates space for subjective claims about datasets, and allows metadata
// to take on a higher frequency of change in contrast to the underlying definition.
// In addition, descriptive metadata can and should be author attributed
// associating descriptive claims about a resource with a cyptographic keypair which
// may represent a person, group of people, or software.
// This metadata format is also subject to massive amounts of change.
// Design goals should include making this compatible with the DCAT spec,
// with the one major exception that hashes are acceptable in place of urls.
type Dataset struct {
	// private storage for reference to this object
	path datastore.Key

	// Kind is required, must be qri:ds:[version]
	Kind Kind `json:"kind"`

	// Time this dataset was created. Required. Datasets are immutable, so no "updated"
	Timestamp time.Time `json:"timestamp"`
	// Structure of this dataset, required
	Structure *Structure `json:"structure"`
	// AbstractStructure is the abstract form of the structure field
	AbstractStructure *Structure `json:"abstractStructure,omitempty"`

	// Data is the path to the hash of raw data as it resolves on the network.
	Data datastore.Key `json:"data"`
	// Length is the length of the data object in bytes.
	// must always match & be present
	Length int `json:"length"`
	// number of rows in the dataset.
	// required and must match underlying dataset.
	Rows int `json:"rows"`
	// Previous connects datasets to form a historical DAG
	Previous datastore.Key `json:"previous,omitempty"`
	// Commit contains author & change message information
	Commit *CommitMsg `json:"commit"`

	// Title of this dataset
	Title string `json:"title,omitempty"`
	// Url to access the dataset
	AccessURL string `json:"accessUrl,omitempty"`
	// Url that should / must lead directly to the data itself
	DownloadURL string `json:"downloadUrl,omitempty"`
	// The frequency with which dataset changes. Must be an ISO 8601 repeating duration
	AccrualPeriodicity string `json:"accrualPeriodicity,omitempty"`
	// path to readme
	Readme datastore.Key `json:"readme,omitempty"`
	// Author
	Author    *User       `json:"author,omitempty"`
	Citations []*Citation `json:"citations"`
	Image     string      `json:"image,omitempty"`
	// Description follows the DCAT sense of the word, it should be around a paragraph of
	// human-readable text
	Description string `json:"description,omitempty"`
	Homepage    string `json:"homepage,omitempty"`
	IconImage   string `json:"iconImage,omitempty"`
	// Identifier is for *other* data catalog specifications. Identifier should not be used
	// or relied on to be unique, because this package does not enforce any of these rules.
	Identifier string `json:"identifier,omitempty"`
	// License will automatically parse to & from a string value if provided as a raw string
	License *License `json:"license,omitempty"`
	// SemVersion this dataset?
	Version string `json:"version,omitempty"`
	// String of Keywords
	Keywords []string `json:"keywords,omitempty"`
	// Contribute
	Contributors []*User `json:"contributors,omitempty"`
	// Languages this dataset is written in
	Language []string `json:"language,omitempty"`
	// Theme
	Theme []string `json:"theme,omitempty"`

	// QueryString is the user-inputted string of an SQL transform
	QueryString string `json:"queryString,omitempty"`

	// Transform is a path to the transformation that generated this resource
	Transform *Transform `json:"transform,omitempty"`
	// AbstractTransform is a reference to the general form of the transform this dataset represents
	AbstractTransform *AbstractTransform `json:"abstractTransform,omitempty"`

	// meta holds additional arbitrarty metadata not covered by the spec
	// when encoding & decoding json values here will be hoisted into the
	// Dataset object
	meta map[string]interface{}
}

// IsEmpty checks to see if dataset has any fields other than the internal path
func (ds *Dataset) IsEmpty() bool {
	return ds.Title == "" && ds.Description == "" && ds.Structure == nil && ds.Timestamp.IsZero() && ds.Previous.String() == ""
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

// Meta gives access to additional metadata not covered by dataset metadata
func (ds *Dataset) Meta() map[string]interface{} {
	if ds.meta == nil {
		ds.meta = map[string]interface{}{}
	}
	return ds.meta
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
		if !d.Timestamp.IsZero() {
			ds.Timestamp = d.Timestamp
		}

		if ds.Structure == nil && d.Structure != nil {
			ds.Structure = d.Structure
		} else if ds.Structure != nil {
			ds.Structure.Assign(d.Structure)
		}

		if ds.AbstractStructure == nil && d.AbstractStructure != nil {
			ds.AbstractStructure = d.AbstractStructure
		} else if ds.AbstractStructure != nil {
			ds.AbstractStructure.Assign(d.AbstractStructure)
		}

		if d.Data.String() != "" {
			ds.Data = d.Data
		}
		if d.Length != 0 {
			ds.Length = d.Length
		}
		if d.Previous.String() != "" {
			ds.Previous = d.Previous
		}
		ds.Commit.Assign(d.Commit)
		if d.Title != "" {
			ds.Title = d.Title
		}
		if d.AccessURL != "" {
			ds.AccessURL = d.AccessURL
		}
		if d.DownloadURL != "" {
			ds.DownloadURL = d.DownloadURL
		}
		if d.Readme.String() != "" {
			ds.Readme = d.Readme
		}
		if d.Author != nil {
			ds.Author = d.Author
		}
		if d.AccrualPeriodicity != "" {
			ds.AccrualPeriodicity = d.AccrualPeriodicity
		}
		if d.Citations != nil {
			ds.Citations = d.Citations
		}
		if d.Image != "" {
			ds.Image = d.Image
		}
		if d.Description != "" {
			ds.Description = d.Description
		}
		if d.Homepage != "" {
			ds.Homepage = d.Homepage
		}
		if d.IconImage != "" {
			ds.IconImage = d.IconImage
		}
		if d.Identifier != "" {
			ds.Identifier = d.Identifier
		}
		if d.License != nil {
			ds.License = d.License
		}
		if d.Version != "" {
			ds.Version = d.Version
		}
		if d.Keywords != nil {
			ds.Keywords = d.Keywords
		}
		if d.Contributors != nil {
			ds.Contributors = d.Contributors
		}
		if d.Language != nil {
			ds.Language = d.Language
		}
		if d.Theme != nil {
			ds.Theme = d.Theme
		}
		if d.QueryString != "" {
			ds.QueryString = d.QueryString
		}
		if d.Transform != nil {
			ds.Transform = d.Transform
		}
		if d.meta != nil {
			ds.meta = d.meta
		}
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

	data := ds.Meta()
	if ds.AbstractTransform != nil {
		data["abstractTransform"] = ds.AbstractTransform
	}
	if ds.AbstractStructure != nil {
		data["abstractStructure"] = ds.AbstractStructure
	}
	if ds.AccessURL != "" {
		data["accessUrl"] = ds.AccessURL
	}
	if ds.Author != nil {
		data["author"] = ds.Author
	}
	if ds.Citations != nil {
		data["citations"] = ds.Citations
	}
	if ds.Contributors != nil {
		data["contributors"] = ds.Contributors
	}
	data["data"] = ds.Data
	if ds.Description != "" {
		data["description"] = ds.Description
	}
	if ds.DownloadURL != "" {
		data["downloadUrl"] = ds.DownloadURL
	}
	if ds.Homepage != "" {
		data["homepage"] = ds.Homepage
	}
	if ds.IconImage != "" {
		data["iconImage"] = ds.IconImage
	}
	if ds.Identifier != "" {
		data["identifier"] = ds.Identifier
	}
	if ds.Image != "" {
		data["image"] = ds.Image
	}
	if ds.Keywords != nil {
		data["keywords"] = ds.Keywords
	}
	data["kind"] = DatasetKind
	if ds.Language != nil {
		data["language"] = ds.Language
	}
	data["length"] = ds.Length
	if ds.License != nil {
		data["license"] = ds.License
	}
	if ds.Previous.String() != "" {
		data["previous"] = ds.Previous
	}
	if ds.Commit != nil {
		data["commit"] = ds.Commit
	}
	if ds.Transform != nil {
		data["transform"] = ds.Transform
	}
	if ds.QueryString != "" {
		data["queryString"] = ds.QueryString
	}
	if ds.Readme.String() != "" {
		data["readme"] = ds.Readme
	}
	data["structure"] = ds.Structure
	if ds.Theme != nil {
		data["theme"] = ds.Theme
	}
	data["timestamp"] = ds.Timestamp
	data["title"] = ds.Title
	if ds.AccrualPeriodicity != "" {
		data["accrualPeriodicity"] = ds.AccrualPeriodicity
	}
	if ds.Version != "" {
		data["version"] = ds.Version
	}

	return json.Marshal(data)
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

	meta := map[string]interface{}{}
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("error unmarshaling dataset metadata: %s", err, err)
	}

	for _, f := range []string{
		"abstractTransform",
		"abstractStructure",
		"accessUrl",
		"accrualPeriodicity",
		"author",
		"citations",
		"commit",
		"contributors",
		"data",
		"description",
		"downloadUrl",
		"homepage",
		"iconImage",
		"identifier",
		"image",
		"keywords",
		"kind",
		"language",
		"length",
		"license",
		"previous",
		"transform",
		"queryString",
		"readme",
		"structure",
		"theme",
		"timestamp",
		"title",
		"version",
	} {
		delete(meta, f)
	}

	d.meta = meta
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
		return nil, fmt.Errorf("couldn't parse dataset, value is invalid type")
	}
}

// CompareDatasets checks if all fields of a dataset are equal,
// returning an error on the first mismatch, nil if equal
func CompareDatasets(a, b *Dataset) error {
	if a.Title != b.Title {
		return fmt.Errorf("Title mismatch: %s != %s", a.Title, b.Title)
	}

	// if err := compare.MapStringInterface(a.Meta(), b.Meta()); err != nil {
	// 	return fmt.Errorf("meta mismatch: %s", err.Error())
	// }

	if a.AccessURL != b.AccessURL {
		return fmt.Errorf("accessUrl mismatch: %s != %s", a.AccessURL, b.AccessURL)
	}
	if a.Readme != b.Readme {
		return fmt.Errorf("Readme mismatch: %s != %s", a.Readme, b.Readme)
	}
	if a.Author != b.Author {
		return fmt.Errorf("Author mismatch: %s != %s", a.Author, b.Author)
	}
	if a.Image != b.Image {
		return fmt.Errorf("Image mismatch: %s != %s", a.Image, b.Image)
	}
	if a.Description != b.Description {
		return fmt.Errorf("Description mismatch: %s != %s", a.Description, b.Description)
	}
	if a.Homepage != b.Homepage {
		return fmt.Errorf("Homepage mismatch: %s != %s", a.Homepage, b.Homepage)
	}
	if a.IconImage != b.IconImage {
		return fmt.Errorf("IconImage mismatch: %s != %s", a.IconImage, b.IconImage)
	}
	if a.DownloadURL != b.DownloadURL {
		return fmt.Errorf("DownloadURL mismatch: %s != %s", a.DownloadURL, b.DownloadURL)
	}
	if a.AccrualPeriodicity != b.AccrualPeriodicity {
		return fmt.Errorf("AccrualPeriodicity mismatch: %s != %s", a.AccrualPeriodicity, b.AccrualPeriodicity)
	}
	// if err := CompareLicense(a.License, b.License); err != nil {
	// 	return err
	// }
	if a.Version != b.Version {
		return fmt.Errorf("Version mismatch: %s != %s", a.Version, b.Version)
	}
	if len(a.Keywords) != len(b.Keywords) {
		return fmt.Errorf("Keyword length mismatch: %s != %s", len(a.Keywords), len(b.Keywords))
	}
	// if a.Contributors != b.Contributors {
	//  return fmt.Errorf("Contributors mismatch: %s != %s", a.Contributors, b.Contributors)
	// }
	if err := CompareCommitMsgs(a.Commit, b.Commit); err != nil {
		return fmt.Errorf("Commit mismatch: %s", err.Error())
	}
	return nil
}
