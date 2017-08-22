package ipld

import (
	ipld "github.com/ipfs/go-ipld"
)

type Dataset struct {
	// Time this dataset was created. Required. Datasets are immutable, so no "updated"
	Timestamp time.Time `json:"timestamp"`
	// Structure of this dataset, required
	Structure ipld.Link `json:"structure"`
	// Data is the path to the hash of raw data as it resolves on the network.
	Data ipld.Link `json:"data"`
	// Length is the length of the data object in bytes.
	// must always match & be present
	Length int `json:"length"`
	// Previous connects datasets to form a historical DAG
	Previous ipld.Link `json:"previous,omitempty"`
	// Title of this dataset
	Title string `json:"title,omitempty"`
	Url   string `json:"url,omitempty"`
	// path to readme
	Readme datastore.Key `json:"readme,omitempty"`
	// Author
	Author      *User       `json:"author,omitempty"`
	Citations   []*Citation `json:"citations"`
	Image       string      `json:"image,omitempty"`
	Description string      `json:"description,omitempty"`
	Homepage    string      `json:"homepage,omitempty"`
	IconImage   string      `json:"iconImage,omitempty"`
	//
	PosterImage string `json:"posterImage,omitempty"`
	// License
	License *License `json:"license,omitempty"`
	// SemVersion this dataset?
	Version VersionNumber `json:"version,omitempty"`
	// String of Keywords
	Keywords []string `json:"keywords,omitempty"`
	// Contribute
	Contributors []*User `json:"contributors,omitempty"`
	// QueryString is the user-inputted string of this query
	QueryString string `json:"queryString,omitempty"`
	// Query is a path to a query that generated this resource
	Query ipld.Link `json:"query,omitempty"`
	// Syntax this query was written in
	QuerySyntax string `json:"querySyntax"`
	// queryPlatform is an identifier for the operating system that performed the query
	QueryPlatform string `json:"queryPlatform,omitempty"`
	// QueryEngine is an identifier for the application that produced the result
	QueryEngine string `json:"queryEngine,omitempty"`
	// QueryEngineConfig outlines any configuration that would affect the resulting hash
	QueryEngineConfig map[string]interface{} `json:"queryEngineConfig,omitempty`
	// Resources is a map of dataset names to dataset references this query is derived from
	// all tables referred to in the query should be present here
	Resources map[string]ipld.Link `json:"resources,omitempty"`
	// meta holds additional arbitrarty metadata not covered by the spec
	// when encoding & decoding json values here will be hoisted into the
	// Dataset object
	Meta map[string]interface{}
}

func (ds *Dataset) IPLDValidate() bool {
	// check previous is a dataset
	// check that query is a query link
	// data should point to raw data, length should match byte length
	// structure should be a structure

	return true
}

type Resources map[string]ipld.Link

func (rs Resources) IPLDValidate() bool {
	// resources should be a map of name:dataset, where all names are valid
	return true
}
