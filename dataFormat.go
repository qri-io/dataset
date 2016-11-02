package dataset

import (
	"encoding/json"
	"fmt"
)

// DataFormat represents different types of data
type DataFormat int

const (
	UnknownDataFormat DataFormat = iota
	CsvDataFormat
	JsonDataFormat
	XmlDataFormat
	XlsDataFormat
	// TODO - make this list more exhaustive
)

// String implements stringer interface for DataFormat
func (f DataFormat) String() string {
	s, ok := map[DataFormat]string{
		UnknownDataFormat: "",
		CsvDataFormat:     "csv",
		JsonDataFormat:    "json",
		XmlDataFormat:     "xml",
		XlsDataFormat:     "xls",
	}[f]

	if !ok {
		return ""
	}

	return s
}

func (f DataFormat) MarshalJSON() ([]byte, error) {
	if f == UnknownDataFormat {
		return nil, nil
	}
	return []byte(fmt.Sprintf(`"%s"`, f.String())), nil
}

func (f *DataFormat) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Filed type should be a string, got %s", data)
	}

	got, ok := map[string]DataFormat{
		"":     UnknownDataFormat,
		"csv":  CsvDataFormat,
		"json": JsonDataFormat,
		"xml":  XmlDataFormat,
		"xls":  XlsDataFormat,
	}[s]
	if !ok {
		return fmt.Errorf("invalid DataFormat %q", s)
	}

	*f = got
	return nil
}
