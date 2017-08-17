package dataset

import (
	"github.com/ipfs/go-datastore"
	"time"
)

// Dataset is stored separately from prescriptive metadata stored in Resource structs
// to maximize overlap of the formal query & resource definitions.
// This also creates space for subjective claims about datasets, and allows metadata
// to take on a higher frequency of change in contrast to the underlying definition.
// In addition, descriptive metadata can and should be author attributed
// associating descriptive claims about a resource with a cyptographic keypair which
// may represent a person, group of people, or software.
// This metadata format is also subject to massive amounts of change.
// Design goals should include making this compatible with the DCAT spec,
// with the one major exception that hashes are acceptable in place of urls.
type Dataset struct {
	Title        string        `json:"title,omitempty"`
	Url          string        `json:"url,omitempty"`
	Readme       string        `json:"readme,omitempty"`
	Author       *User         `json:"author,omitempty"`
	Image        string        `json:"image,omitempty"`
	Description  string        `json:"description,omitempty"`
	Homepage     string        `json:"homepage,omitempty"`
	IconImage    string        `json:"icon_image,omitempty"`
	PosterImage  string        `json:"poster_image,omitempty"`
	License      *License      `json:"license,omitempty"`
	Version      VersionNumber `json:"version,omitempty"`
	Keywords     []string      `json:"keywords,omitempty"`
	Contributors []*User       `json:"contributors,omitempty"`
	// Time this dataset was created. Required. Datasets are immutable, so no "updated"
	Timestamp time.Time `json:"timestamp"`
	// Length is the length of the source data in bytes
	// must always match & be present
	Length int `json:"length"`
	// Previous connects datasets to form a history
	Previous datastore.Key `json:"previous,omitempty"`
	// Data is the path to the hash of raw data as it resolves on the network.
	Data datastore.Key `json:"data"`
	// Query is a path to a query that generated this resource
	Query datastore.Key `json:"query,omitempty"`
	// queryPlatform is an identifier for the operating system that performed the query
	QueryPlatform string `json:"queryPlatform,omitempty"`
	// QueryEngine is an identifier for the application that produced the result
	QueryEngine string `json:"queryEngine,omitempty"`
	// QueryEngineConfig outlines any configuration that would affect the resulting hash
	QueryEngineConfig map[string]interface{} `json:"queryEngineConfig,omitempty`
	// Resources is a list
	Resources map[string]datastore.Key `json:"resources,omitempty"`
}

// separate type for marshalling into & out of
// most importantly, struct names must be sorted lexographically
type _dataset struct {
}
