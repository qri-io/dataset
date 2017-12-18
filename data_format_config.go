package dataset

import (
	"fmt"
)

// FormatConfig is the interface for data format configurations
type FormatConfig interface {
	// Format gives the data format being configured
	Format() DataFormat
	// map gives an object of configuration details
	Map() map[string]interface{}
}

// ParseFormatConfigMap returns a FormatConfig implementation for a given data format
// and options map, often used in decoding from recorded formats like, say, JSON
func ParseFormatConfigMap(f DataFormat, opts map[string]interface{}) (FormatConfig, error) {
	switch f {
	case CSVDataFormat:
		return NewCSVOptions(opts)
	case JSONDataFormat:
		return NewJSONOptions(opts)
	default:
		return nil, fmt.Errorf("cannot parse configuration for format: %s", f.String())
	}
}

// NewCSVOptions creates a CSVOptions pointer from a map
func NewCSVOptions(opts map[string]interface{}) (FormatConfig, error) {
	o := &CSVOptions{}
	if opts == nil {
		return o, nil
	}
	if opts["headerRow"] != nil {
		if headerRow, ok := opts["headerRow"].(bool); ok {
			o.HeaderRow = headerRow
		} else {
			return nil, fmt.Errorf("invalid headerRow value: %s", opts["headerRow"])
		}
	}

	return o, nil
}

// CSVOptions specifies configuration details for csv files
// This'll expand in the future to interoperate with okfn csv spec
type CSVOptions struct {
	// HeaderRow specifies weather this csv file has a header row or not
	HeaderRow bool `json:"headerRow"`
}

// Format announces the CSV Data Format for the FormatConfig interface
func (*CSVOptions) Format() DataFormat {
	return CSVDataFormat
}

// Map returns a map[string]interface representation of the configuration
func (o *CSVOptions) Map() map[string]interface{} {
	if o == nil {
		return nil
	}
	return map[string]interface{}{
		"headerRow": o.HeaderRow,
	}
}

// NewJSONOptions creates a JSONOptions pointer from a map
func NewJSONOptions(opts map[string]interface{}) (FormatConfig, error) {
	o := &JSONOptions{}
	if opts == nil {
		return o, nil
	}
	if opts["arrayEntries"] != nil {
		if arrayEntries, ok := opts["arrayEntries"].(bool); ok {
			o.ArrayEntries = arrayEntries
		} else {
			return nil, fmt.Errorf("invalid arrayEntries value: %s", opts["arrayEntries"])
		}
	}
	return o, nil
}

// JSONOptions specifies configuration details for json files
// note that is for treating json files as a *dataset*, not
// the JSON datatype from the github.com/qri-io/dataset/datatypes
// package
type JSONOptions struct {
	ArrayEntries bool `json:"arrayEntries"`
}

// Format announces the JSON Data Format for the FormatConfig interface
func (*JSONOptions) Format() DataFormat {
	return JSONDataFormat
}

// Map returns a map[string]interface representation of the configuration
func (o *JSONOptions) Map() map[string]interface{} {
	if o == nil {
		return nil
	}
	return map[string]interface{}{
		"arrayEntries": o.ArrayEntries,
	}
}
