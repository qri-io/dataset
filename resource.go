package dataset

import (
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset/compression"
)

// Resource designates a deterministic definition for working with a discrete dataset
// Resource is concrete handle that provides precise details about how to interpret the underlying data.
// A Resource must resolve to one and only one entity, specified by a `path` property in the resource definition.
// These techniques provide mechanisms for joining & traversing multiple resources.
// This example is shown in a human-readable form, for storage on the network the actual
// output would be in a condensed, non-indented form, with keys sorted by lexographic order.
type Resource struct {
	// Format specifies the format of the raw data MIME type
	Format DataFormat `json:"format"`
	// FormatConfig removes as much ambiguity as possible about how
	// to interpret the speficied format.
	FormatConfig FormatConfig `json:"formatConfig,omitempty"`
	// Encoding specifics character encoding
	// should assume utf-8 if not specified
	Encoding string `json:"encoding,omitempty"`
	// Length is the length of the source data in bytes
	// must always match & be present
	Length int `json:"length"`
	// Compression specifies any compression on the source data,
	// if empty assume no compression
	Compression compression.Type `json:"compression,omitempty"`
	// Schema contains the schema definition for the underlying data
	Schema *Schema `json:"schema"`
	// Path is the path to the hash of raw data as it resolves on the network.
	Path datastore.Key `json:"path"`
	// Query is a path to a query that generated this resource
	Query datastore.Key `json:"query,omitempty"`
	// queryPlatform is an identifier for the operating system that performed the query
	QueryPlatform string `json:"queryPlatform,omitempty"`
	// QueryEngine is an identifier for the application that produced the result
	QueryEngine string `json:"queryEngine,omitempty"`
	// QueryEngineConfig outlines any configuration that would affect the resulting hash
	QueryEngineConfig map[string]interface{} `json:"queryEngineConfig,omitempty`
}

// Hash gives the hash of this resource
func (r *Resource) Hash() (string, error) {
	return JSONHash(r)
}

// truthCount returns the number of arguments that are true
func truthCount(args ...bool) (count int) {
	for _, arg := range args {
		if arg {
			count++
		}
	}
	return
}

// separate type for marshalling into & out of
// most importantly, struct names must be sorted lexographically
type _resource struct {
	Compression       compression.Type       `json:"compression"`
	Encoding          string                 `json:"encoding"`
	Format            DataFormat             `json:"format"`
	FormatConfig      map[string]interface{} `json:"formatOptions"`
	Length            int                    `json:"length"`
	Path              datastore.Key          `json:"path"`
	Query             datastore.Key          `json:"query,omitempty"`
	QueryEngine       string                 `json:"queryEngine,omitempty"`
	QueryEngineConfig map[string]interface{} `json:"queryEngineConfig,omitempty`
	QueryPlatform     string                 `json:"queryPlatform,omitempty"`
	Schema            *Schema                `json:"schema,omitempty"`
}

// MarshalJSON satisfies the json.Marshaler interface
func (r Resource) MarshalJSON() (data []byte, err error) {
	var opt map[string]interface{}
	if r.FormatConfig != nil {
		opt = r.FormatConfig.Map()
	}

	return json.Marshal(&_resource{
		Compression:       r.Compression,
		Encoding:          r.Encoding,
		Format:            r.Format,
		FormatConfig:      opt,
		Length:            r.Length,
		Path:              r.Path,
		Query:             r.Query,
		QueryEngine:       r.QueryEngine,
		QueryEngineConfig: r.QueryEngineConfig,
		QueryPlatform:     r.QueryPlatform,
		Schema:            r.Schema,
	})
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (r *Resource) UnmarshalJSON(data []byte) error {
	_r := &_resource{}
	if err := json.Unmarshal(data, _r); err != nil {
		return err
	}

	fmtCfg, err := ParseFormatConfigMap(_r.Format, _r.FormatConfig)
	if err != nil {
		return err
	}

	*r = Resource{
		Compression:       _r.Compression,
		Encoding:          _r.Encoding,
		Format:            _r.Format,
		FormatConfig:      fmtCfg,
		Length:            _r.Length,
		Path:              _r.Path,
		Query:             _r.Query,
		QueryEngine:       _r.QueryEngine,
		QueryEngineConfig: _r.QueryEngineConfig,
		QueryPlatform:     _r.QueryPlatform,
		Schema:            _r.Schema,
	}

	// TODO - question of weather we should not accept
	// invalid resource defs at parse time. For now we'll take 'em.
	// if err := d.Valid(); err != nil {
	//   return err
	// }

	// errs := AddressErrors(d, &[]Address{})
	// if len(errs) > 0 {
	//   return errs[0]
	// }

	return nil
}

// Valid validates weather or not this resource
func (ds *Resource) Valid() error {
	// if count := truthCount(ds.Url != "", ds.File != "", len(ds.Data) > 0); count > 1 {
	// 	return errors.New("only one of url, file, or data can be set")
	// } else if count == 1 {
	// 	if ds.Format == UnknownDataFormat {
	// 		// if format is unspecified, we need to be able to derive the format from
	// 		// the extension of either the url or filepath
	// 		if ds.DataFormat() == "" {
	// 			return errors.New("format is required for data source")
	// 		}
	// 	}
	// }

	return nil
}

// UnmarshalResource tries to extract a resource type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalResource(v interface{}) (*Resource, error) {
	switch r := v.(type) {
	case *Resource:
		return r, nil
	case Resource:
		return &r, nil
	case []byte:
		resource := &Resource{}
		err := json.Unmarshal(r, resource)
		return resource, err
	default:
		return nil, fmt.Errorf("couldn't parse resource")
	}
}
