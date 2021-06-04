package dataset

import (
	"encoding/json"
	"fmt"
)

// ErrUnknownDataFormat is the expected error for
// when a data format is missing or unknown
var ErrUnknownDataFormat = fmt.Errorf("Unknown Data Format")

// DataFormat represents different types of data formats.
// formats specified here have some degree of support within
// the dataset packages
type DataFormat int

const (
	// UnknownDataFormat is the default dataformat, meaning
	// that a data format should always be specified when
	// using the DataFormat type
	UnknownDataFormat DataFormat = iota
	// CSVDataFormat specifies comma separated value-formatted data
	CSVDataFormat
	// JSONDataFormat specifies Javascript Object Notation-formatted data
	JSONDataFormat
	// NDJSONDataFormat newline-delimited JSON files
	// https://github.com/ndjson/ndjson-spec
	NDJSONDataFormat
	// CBORDataFormat specifies RFC 7049 Concise Binary Object Representation
	// read more at cbor.io
	CBORDataFormat
	// XMLDataFormat specifies eXtensible Markup Language-formatted data
	// currently not supported.
	XMLDataFormat
	// XLSXDataFormat specifies microsoft excel formatted data
	XLSXDataFormat
)

// SupportedDataFormats gives a slice of data formats that are
// expected to work with this dataset package. As we work through
// support for different formats, the last step of providing full
// support to a format will be an addition to this slice
func SupportedDataFormats() []DataFormat {
	return []DataFormat{
		CBORDataFormat,
		JSONDataFormat,
		CSVDataFormat,
		XLSXDataFormat,
		NDJSONDataFormat,
	}
}

// String implements stringer interface for DataFormat
func (f DataFormat) String() string {
	s, ok := map[DataFormat]string{
		UnknownDataFormat: "",
		CSVDataFormat:     "csv",
		JSONDataFormat:    "json",
		XMLDataFormat:     "xml",
		XLSXDataFormat:    "xlsx",
		CBORDataFormat:    "cbor",
		NDJSONDataFormat:  "ndjson",
	}[f]

	if !ok {
		return ""
	}

	return s
}

// ParseDataFormatString takes a string representation of a data format
// TODO (b5): trim "." prefix, remove prefixed map keys
func ParseDataFormatString(s string) (df DataFormat, err error) {
	df, ok := map[string]DataFormat{
		"":        UnknownDataFormat,
		".csv":    CSVDataFormat,
		"csv":     CSVDataFormat,
		".json":   JSONDataFormat,
		"json":    JSONDataFormat,
		".xml":    XMLDataFormat,
		"xml":     XMLDataFormat,
		".xlsx":   XLSXDataFormat,
		"xlsx":    XLSXDataFormat,
		"cbor":    CBORDataFormat,
		".cbor":   CBORDataFormat,
		".ndjson": NDJSONDataFormat,
		"ndjson":  NDJSONDataFormat,
		".jsonl":  NDJSONDataFormat,
		"jsonl":   NDJSONDataFormat,
	}[s]
	if !ok {
		err = fmt.Errorf("invalid data format: `%s`", s)
		df = UnknownDataFormat
	}

	return
}

// MarshalJSON satisfies the json.Marshaler interface
func (f DataFormat) MarshalJSON() ([]byte, error) {
	if f == UnknownDataFormat {
		return nil, ErrUnknownDataFormat
	}
	return []byte(fmt.Sprintf(`"%s"`, f.String())), nil
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (f *DataFormat) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Data Format type should be a string, got %s", data)
	}

	df, err := ParseDataFormatString(s)
	if err != nil {
		return err
	}

	*f = df
	return nil
}
