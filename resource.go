package dataset

import (
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset/compression"
	// "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

type ResourceLink struct {
	Structure datastore.Key
	Data      datastore.Key
}

type Resource struct {
	Structure
	Data datastore.Key
}

// Structure designates a deterministic definition for working with a discrete dataset.
// Structure is a concrete handle that provides precise details about how to interpret the underlying data.
// A Structure must resolve to one and only one entity, specified by a `path` property in the structure definition.
// These techniques provide mechanisms for joining & traversing multiple structures.
// This example is shown in a human-readable form, for storage on the network the actual
// output would be in a condensed, non-indented form, with keys sorted by lexographic order.
type Structure struct {
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
}

// Hash gives the hash of this structure
func (r *Structure) Hash() (string, error) {
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

// MarshalJSON satisfies the json.Marshaler interface
func (r Structure) MarshalJSON() (data []byte, err error) {
	var opt map[string]interface{}
	if r.FormatConfig != nil {
		opt = r.FormatConfig.Map()
	}

	return json.Marshal(&_structure{
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

// separate type for marshalling into & out of
// most importantly, struct names must be sorted lexographically
type _structure struct {
	Compression       compression.Type       `json:"compression,omitempty"`
	Encoding          string                 `json:"encoding,omitempty"`
	Format            DataFormat             `json:"format"`
	FormatConfig      map[string]interface{} `json:"formatConfig,omitempty"`
	Length            int                    `json:"length,omitempty"`
	Path              datastore.Key          `json:"path,omitempty"`
	Query             datastore.Key          `json:"query,omitempty"`
	QueryEngine       string                 `json:"queryEngine,omitempty"`
	QueryEngineConfig map[string]interface{} `json:"queryEngineConfig,omitempty"`
	QueryPlatform     string                 `json:"queryPlatform,omitempty"`
	Schema            *Schema                `json:"schema,omitempty"`
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (r *Structure) UnmarshalJSON(data []byte) error {
	_r := &_structure{}
	if err := json.Unmarshal(data, _r); err != nil {
		return err
	}

	fmtCfg, err := ParseFormatConfigMap(_r.Format, _r.FormatConfig)
	if err != nil {
		return err
	}

	*r = Structure{
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
	// invalid structure defs at parse time. For now we'll take 'em.
	// if err := d.Valid(); err != nil {
	//   return err
	// }

	// errs := AddressErrors(d, &[]Address{})
	// if len(errs) > 0 {
	//   return errs[0]
	// }

	return nil
}

// Valid validates weather or not this structure
func (ds *Structure) Valid() error {
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

// UnmarshalStructure tries to extract a structure type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalStructure(v interface{}) (*Structure, error) {
	switch r := v.(type) {
	case *Structure:
		return r, nil
	case Structure:
		return &r, nil
	case []byte:
		structure := &Structure{}
		err := json.Unmarshal(r, structure)
		return structure, err
	default:
		return nil, fmt.Errorf("couldn't parse structure")
	}
}
