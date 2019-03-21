package dataset

import (
	"encoding/json"
	"fmt"

	"github.com/qri-io/jsonschema"
)

var (
	// BaseSchemaArray is a minimum schema to constitute a dataset, specifying
	// the top level of the document is an array
	BaseSchemaArray = map[string]interface{}{"type": "array"}
	// BaseSchemaObject is a minimum schema to constitute a dataset, specifying
	// the top level of the document is an object
	BaseSchemaObject = map[string]interface{}{"type": "object"}
)

// Structure defines the characteristics of a dataset document necessary for a
// machine to interpret the dataset body.
// Structure fields are things like the encoding data format (JSON,CSV,etc.),
// length of the dataset body in bytes, stored in a rigid form intended for
// machine use. A well defined structure & accompanying software should
// allow the end user to spend more time focusing on the data itself
// Two dataset documents that both have a defined structure will have some
// degree of natural interoperability, depending first on the amount of detail
// provided in a dataset's structure, and then by the natural comparibilty of
// the datasets
type Structure struct {
	// Checksum is a bas58-encoded multihash checksum of the entire data
	// file this structure points to. This is different from IPFS
	// hashes, which are calculated after breaking the file into blocks
	Checksum string `json:"checksum,omitempty"`
	// Compression specifies any compression on the source data,
	// if empty assume no compression
	Compression string `json:"compression,omitempty"`
	// Maximum nesting level of composite types in the dataset. eg: depth 1 == [], depth 2 == [[]]
	Depth int `json:"depth,omitempty"`
	// Encoding specifics character encoding, assume utf-8 if not specified
	Encoding string `json:"encoding,omitempty"`
	// ErrCount is the number of errors returned by validating data
	// against this schema. required
	ErrCount int `json:"errCount"`
	// Entries is number of top-level entries in the dataset. With tablular data
	// this is the same as the number of "rows"
	Entries int `json:"entries,omitempty"`
	// Format specifies the format of the raw data MIME type
	Format string `json:"format"`
	// FormatConfig removes as much ambiguity as possible about how
	// to interpret the speficied format.
	// FormatConfig FormatConfig `json:"formatConfig,omitempty"`
	FormatConfig map[string]interface{} `json:"formatConfig,omitempty"`

	// Length is the length of the data object in bytes.
	// must always match & be present
	Length int `json:"length,omitempty"`
	// location of this structure, transient
	Path string `json:"path,omitempty"`
	// Qri should always be KindStructure
	Qri string `json:"qri"`
	// Schema contains the schema definition for the underlying data, schemas
	// are defined using the IETF json-schema specification. for more info
	// on json-schema see: https://json-schema.org
	Schema map[string]interface{} `json:"schema,omitempty"`
}

// NewStructureRef creates an empty struct with it's
// internal path set
func NewStructureRef(path string) *Structure {
	return &Structure{Qri: KindStructure.String(), Path: path}
}

// DropTransientValues removes values that cannot be recorded when the
// dataset is rendered immutable, usually by storing it in a cafs
func (s *Structure) DropTransientValues() {
	s.Path = ""
}

// JSONSchema parses the Schema field into a json-schema
func (s *Structure) JSONSchema() (*jsonschema.RootSchema, error) {
	// TODO (b5): SLOW. we should teach the jsonschema package to parse native go types,
	// replacing this nonsense. Someone's even filed an issue on regarding this:
	// https://github.comqri-io/jsonschema/issues/32
	data, err := json.Marshal(s.Schema)
	if err != nil {
		return nil, err
	}

	rs := &jsonschema.RootSchema{}
	if err := json.Unmarshal(data, rs); err != nil {
		return nil, err
	}

	return rs, nil
}

// DataFormat gives format as a DataFormat type, returning UnknownDataFormat in
// any case where st.DataFormat is an invalid string
func (s *Structure) DataFormat() DataFormat {
	df, _ := ParseDataFormatString(s.Format)
	return df
}

// Abstract returns this structure instance in it's "Abstract" form
// stripping all nonessential values &
// renaming all schema field names to standard variable names
func (s *Structure) Abstract() *Structure {
	a := &Structure{
		Format:       s.Format,
		FormatConfig: s.FormatConfig,
		Encoding:     s.Encoding,
	}
	if s.Schema != nil {
		// TODO - Fix meeeeeeee
		// a.Schema = &Schema{
		// 	PrimaryKey: s.Schema.PrimaryKey,
		// 	Fields:     make([]*Field, len(s.Schema.Fields)),
		// }
		// for i, f := range s.Schema.Fields {
		// 	a.Schema.Fields[i] = &Field{
		// 		Name:         AbstractColumnName(i),
		// 		Type:         f.Type,
		// 		MissingValue: f.MissingValue,
		// 		Format:       f.Format,
		// 		Constraints:  f.Constraints,
		// 	}
		// }
	}
	return a
}

// Hash gives the hash of this structure
func (s *Structure) Hash() (string, error) {
	return JSONHash(s)
}

// separate type for marshalling into & out of
// most importantly, struct names must be sorted lexographically
type _structure Structure

// MarshalJSON satisfies the json.Marshaler interface
func (s Structure) MarshalJSON() (data []byte, err error) {
	if s.Path != "" && s.Encoding == "" && s.Schema == nil {
		return json.Marshal(s.Path)
	}

	return s.MarshalJSONObject()
}

// MarshalJSONObject always marshals to a json Object, even if meta is empty or a reference
func (s Structure) MarshalJSONObject() ([]byte, error) {
	kind := s.Qri
	if kind == "" {
		kind = KindStructure.String()
	}

	var opt map[string]interface{}
	if s.FormatConfig != nil {
		opt = s.FormatConfig
	}

	return json.Marshal(&_structure{
		Checksum:     s.Checksum,
		Compression:  s.Compression,
		Depth:        s.Depth,
		Encoding:     s.Encoding,
		Entries:      s.Entries,
		ErrCount:     s.ErrCount,
		Format:       s.Format,
		FormatConfig: opt,
		Length:       s.Length,
		Qri:          kind,
		Schema:       s.Schema,
	})
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (s *Structure) UnmarshalJSON(data []byte) (err error) {
	var str string

	if err := json.Unmarshal(data, &str); err == nil {
		*s = Structure{Path: str}
		return nil
	}

	_s := _structure{}
	if err := json.Unmarshal(data, &_s); err != nil {
		return fmt.Errorf("error unmarshaling dataset structure from json: %s", err.Error())
	}

	*s = Structure(_s)
	return nil
}

// IsEmpty checks to see if structure has any fields other than the internal path
func (s *Structure) IsEmpty() bool {
	return s.Checksum == "" &&
		s.Compression == "" &&
		s.Depth == 0 &&
		s.Encoding == "" &&
		s.Entries == 0 &&
		s.ErrCount == 0 &&
		s.Format == "" &&
		s.FormatConfig == nil &&
		s.Length == 0 &&
		s.Schema == nil
}

// Assign collapses all properties of a group of structures on to one
// this is directly inspired by Javascript's Object.assign
func (s *Structure) Assign(structures ...*Structure) {
	for _, st := range structures {
		if st == nil {
			continue
		}

		if st.Path != "" {
			s.Path = st.Path
		}
		if st.Checksum != "" {
			s.Checksum = st.Checksum
		}
		if st.Compression != "" {
			s.Compression = st.Compression
		}
		if st.Depth != 0 {
			s.Depth = st.Depth
		}
		if st.Encoding != "" {
			s.Encoding = st.Encoding
		}
		if st.Entries != 0 {
			s.Entries = st.Entries
		}
		if st.ErrCount != 0 {
			s.ErrCount = st.ErrCount
		}
		if st.Format != "" {
			s.Format = st.Format
		}
		if st.FormatConfig != nil {
			s.FormatConfig = st.FormatConfig
		}
		if st.Qri != "" {
			s.Qri = st.Qri
		}
		if st.Length != 0 {
			s.Length = st.Length
		}
		// TODO - fix me
		if st.Schema != nil {
			// if s.Schema == nil {
			// 	s.Schema = &RootSchema{}
			// }
			// s.Schema.Assign(st.Schema)
			s.Schema = st.Schema
		}
	}
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
		err := fmt.Errorf("couldn't parse structure, value is invalid type")
		return nil, err
	}
}

// AbstractColumnName is the "base26" value of a column name
// to make short, sql-valid, deterministic column names
func AbstractColumnName(i int) string {
	return base26(i)
}

// b26chars is a-z, lowercase
const b26chars = "abcdefghijklmnopqrstuvwxyz"

// base26 maps the set of natural numbers
// to letters, using repeating characters to handle values
// greater than 26
func base26(d int) (s string) {
	var cols []int
	if d == 0 {
		return "a"
	}

	for d != 0 {
		cols = append(cols, d%26)
		d = d / 26
	}
	for i := len(cols) - 1; i >= 0; i-- {
		if i != 0 && cols[i] > 0 {
			s += string(b26chars[cols[i]-1])
		} else {
			s += string(b26chars[cols[i]])
		}
	}
	return s
}
