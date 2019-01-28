package dataset

import (
	"encoding/json"
	"fmt"

	"github.com/qri-io/dataset/compression"
	"github.com/qri-io/jsonschema"
)

var (
	// BaseSchemaArray is a minimum schema to constitute a dataset, specifying
	// the top level of the document is an array
	BaseSchemaArray = jsonschema.Must(`{"type":"array"}`)
	// BaseSchemaObject is a minimum schema to constitute a dataset, specifying
	// the top level of the document is an object
	BaseSchemaObject = jsonschema.Must(`{"type":"object"}`)
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
	// private storage for reference to this object
	path string

	// Checksum is a bas58-encoded multihash checksum of the entire data
	// file this structure points to. This is different from IPFS
	// hashes, which are calculated after breaking the file into blocks
	Checksum string `json:"checksum,omitempty"`
	// Compression specifies any compression on the source data,
	// if empty assume no compression
	Compression compression.Type `json:"compression,omitempty"`
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
	Format DataFormat `json:"format"`
	// FormatConfig removes as much ambiguity as possible about how
	// to interpret the speficied format.
	FormatConfig FormatConfig `json:"formatConfig,omitempty"`
	// Length is the length of the data object in bytes.
	// must always match & be present
	Length int `json:"length,omitempty"`
	// Qri should always be KindStructure
	Qri Kind `json:"qri"`
	// Schema contains the schema definition for the underlying data, schemas
	// are defined using the IETF json-schema specification. for more info
	// on json-schema see: https://json-schema.org
	Schema *jsonschema.RootSchema `json:"schema,omitempty"`
}

// Path gives the internal path reference for this structure
func (s *Structure) Path() string {
	return s.path
}

// NewStructureRef creates an empty struct with it's
// internal path set
func NewStructureRef(path string) *Structure {
	return &Structure{Qri: KindStructure, path: path}
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
type _structure struct {
	Checksum     string                 `json:"checksum,omitempty"`
	Compression  compression.Type       `json:"compression,omitempty"`
	Depth        int                    `json:"depth,omitempty"`
	Encoding     string                 `json:"encoding,omitempty"`
	Entries      int                    `json:"entries,omitempty"`
	ErrCount     int                    `json:"errCount"`
	Format       DataFormat             `json:"format"`
	FormatConfig map[string]interface{} `json:"formatConfig,omitempty"`
	Length       int                    `json:"length,omitempty"`
	Qri          Kind                   `json:"qri"`
	Schema       *jsonschema.RootSchema `json:"schema,omitempty"`
}

// MarshalJSON satisfies the json.Marshaler interface
func (s Structure) MarshalJSON() (data []byte, err error) {
	if s.path != "" && s.Encoding == "" && s.Schema == nil {
		return json.Marshal(s.path)
	}

	return s.MarshalJSONObject()
}

// MarshalJSONObject always marshals to a json Object, even if meta is empty or a reference
func (s Structure) MarshalJSONObject() ([]byte, error) {
	kind := s.Qri
	if kind == "" {
		kind = KindStructure
	}

	var opt map[string]interface{}
	if s.FormatConfig != nil {
		opt = s.FormatConfig.Map()
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
	var (
		str    string
		fmtCfg FormatConfig
	)
	if err := json.Unmarshal(data, &str); err == nil {
		*s = Structure{path: str}
		return nil
	}

	_s := &_structure{}
	if err := json.Unmarshal(data, _s); err != nil {
		log.Debug(err.Error())
		return fmt.Errorf("error unmarshaling dataset structure from json: %s", err.Error())
	}

	if _s.FormatConfig != nil {
		fmtCfg, err = ParseFormatConfigMap(_s.Format, _s.FormatConfig)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error parsing structure formatConfig: %s", err.Error())
		}

	}

	*s = Structure{
		Checksum:     _s.Checksum,
		Compression:  _s.Compression,
		Depth:        _s.Depth,
		Encoding:     _s.Encoding,
		Entries:      _s.Entries,
		ErrCount:     _s.ErrCount,
		Format:       _s.Format,
		FormatConfig: fmtCfg,
		Length:       _s.Length,
		Qri:          _s.Qri,
		Schema:       _s.Schema,
	}
	return nil
}

// IsEmpty checks to see if structure has any fields other than the internal path
func (s *Structure) IsEmpty() bool {
	return s.Checksum == "" &&
		s.Compression == compression.None &&
		s.Depth == 0 &&
		s.Encoding == "" &&
		s.Entries == 0 &&
		s.ErrCount == 0 &&
		s.Format == UnknownDataFormat &&
		s.FormatConfig == nil &&
		s.Length == 0 &&
		s.Schema == nil
}

// SetPath sets the internal path property of a Structure
// Use with caution. most callers should never need to call SetPath
func (s *Structure) SetPath(path string) {
	s.path = path
}

// Assign collapses all properties of a group of structures on to one
// this is directly inspired by Javascript's Object.assign
func (s *Structure) Assign(structures ...*Structure) {
	for _, st := range structures {
		if st == nil {
			continue
		}

		if st.path != "" {
			s.path = st.path
		}
		if st.Checksum != "" {
			s.Checksum = st.Checksum
		}
		if st.Compression != compression.None {
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
		if st.Format != UnknownDataFormat {
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
		log.Debug(err.Error())
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

// Encode creates a StructurePod from a Structure instance
func (s Structure) Encode() *StructurePod {
	var (
		sch  map[string]interface{}
		schd []byte
		err  error
	)

	if s.Schema != nil {
		sch = map[string]interface{}{}
		schd, err = json.Marshal(s.Schema)
		if err != nil {
			sch = nil
		}
		if err = json.Unmarshal(schd, &sch); err != nil {
			sch = nil
		}
	}

	cs := &StructurePod{
		Checksum:    s.Checksum,
		Compression: s.Compression.String(),
		Depth:       s.Depth,
		Encoding:    s.Encoding,
		ErrCount:    s.ErrCount,
		Entries:     s.Entries,
		Format:      s.Format.String(),
		Length:      s.Length,
		Path:        s.Path(),
		Qri:         s.Qri.String(),
		Schema:      sch,
	}

	if s.FormatConfig != nil {
		cs.FormatConfig = s.FormatConfig.Map()
	}

	return cs
}

// Decode creates a Stucture from a CodingStructre instance
func (s *Structure) Decode(cs *StructurePod) (err error) {
	dst := Structure{
		Checksum: cs.Checksum,
		Depth:    cs.Depth,
		Encoding: cs.Encoding,
		ErrCount: cs.ErrCount,
		Entries:  cs.Entries,
		Length:   cs.Length,
	}

	if cs.Qri != "" {
		// TODO - this should respond to changes in cs
		dst.Qri = KindStructure
	}

	if dst.Format, err = ParseDataFormatString(cs.Format); err != nil {
		return err
	}

	if cs.FormatConfig != nil {
		if dst.FormatConfig, err = ParseFormatConfigMap(dst.Format, cs.FormatConfig); err != nil {
			return err
		}
	}

	if cs.Schema != nil {
		sch := &jsonschema.RootSchema{}
		data, e := json.Marshal(cs.Schema)
		if e != nil {
			log.Debugf("marshaling schema data: %s", e.Error())
			return e
		}
		if err = json.Unmarshal(data, sch); err != nil {
			log.Debugf("unmarshaling schema: %s", err.Error())
			return
		}
		dst.Schema = sch
	}

	*s = dst
	return nil
}

// StructurePod is a variant of Structure safe for serialization (encoding & decoding)
// to static formats. It uses only simple go types
type StructurePod struct {
	Checksum     string                 `json:"checksum,omitempty"`
	Compression  string                 `json:"compression,omitempty"`
	Depth        int                    `json:"depth,omitempty"`
	Encoding     string                 `json:"encoding,omitempty"`
	ErrCount     int                    `json:"errCount"`
	Entries      int                    `json:"entries,omitempty"`
	Format       string                 `json:"format"`
	FormatConfig map[string]interface{} `json:"formatConfig,omitempty"`
	Length       int                    `json:"length,omitempty"`
	Path         string                 `json:"path,omitempty"`
	Qri          string                 `json:"qri"`
	Schema       map[string]interface{} `json:"schema,omitempty"`
}

// Assign collapses all properties of zero or more StructurePod onto one.
// inspired by Javascript's Object.assign
func (sp *StructurePod) Assign(sps ...*StructurePod) {
	for _, s := range sps {
		if s == nil {
			continue
		}

		if s.Checksum != "" {
			sp.Checksum = s.Checksum
		}
		if s.Depth != 0 {
			sp.Depth = s.Depth
		}
		if s.Compression != "" {
			sp.Compression = s.Compression
		}
		if s.Encoding != "" {
			sp.Encoding = s.Encoding
		}
		if s.ErrCount != 0 {
			sp.ErrCount = s.ErrCount
		}
		if s.Entries != 0 {
			sp.Entries = s.Entries
		}
		if s.Format != "" {
			sp.Format = s.Format
		}
		if s.FormatConfig != nil {
			sp.FormatConfig = s.FormatConfig
		}
		if s.Length != 0 {
			sp.Length = s.Length
		}
		if s.Path != "" {
			sp.Path = s.Path
		}
		if s.Qri != "" {
			sp.Qri = s.Qri
		}
		if s.Schema != nil {
			sp.Schema = s.Schema
		}
	}
}
