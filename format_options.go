package dataset

import (
	"fmt"
)

type FormatOptions interface {
	Format() DataFormat
	Map() map[string]interface{}
}

func ParseFormatOptionsMap(f DataFormat, opts map[string]interface{}) (FormatOptions, error) {
	switch f {
	case CsvDataFormat:
		return NewCsvOptions(opts)
	case JsonDataFormat:
		return NewJsonOptions(opts)
	}

	return nil, nil
}

func NewCsvOptions(opts map[string]interface{}) (FormatOptions, error) {
	o := &CsvOptions{}
	if opts == nil {
		return o, nil
	}
	if opts["header_row"] != nil {
		if headerRow, ok := opts["header_row"].(bool); ok {
			o.HeaderRow = headerRow
		} else {
			return nil, fmt.Errorf("invalid header_row value: %s", opts["header_row"])
		}
	}

	return o, nil
}

type CsvOptions struct {
	// Weather this csv file has a header row or not
	HeaderRow bool `json:"header_row"`
}

func (*CsvOptions) Format() DataFormat {
	return CsvDataFormat
}

func (o *CsvOptions) Map() map[string]interface{} {
	if o == nil {
		return nil
	}
	return map[string]interface{}{
		"header_row": o.HeaderRow,
	}
}

func NewJsonOptions(opts map[string]interface{}) (FormatOptions, error) {
	o := &JsonOptions{}
	if opts == nil {
		return o, nil
	}
	// TODO
	return o, fmt.Errorf("parsing json data format options isn't finished")
}

type JsonOptions struct {
	ObjectEntries bool `json:"object_entries"`
}

func (*JsonOptions) Format() DataFormat {
	return JsonDataFormat
}

func (o *JsonOptions) Map() map[string]interface{} {
	if o == nil {
		return nil
	}
	return map[string]interface{}{
		"object_entries": o.ObjectEntries,
	}
}
