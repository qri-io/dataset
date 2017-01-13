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
	if headerRow, ok := opts["headerRow"].(bool); ok {
		o.HeaderRow = headerRow
	} else {
		return nil, fmt.Errorf("invalid headerRow value: %s", opts["headerRow"])
	}

	return o, nil
}

type CsvOptions struct {
	// Weather this csv file has a header row or not
	HeaderRow bool `json:"headerRow"`
}

func (*CsvOptions) Format() DataFormat {
	return CsvDataFormat
}

func (o *CsvOptions) Map() map[string]interface{} {
	if o == nil {
		return nil
	}
	return map[string]interface{}{
		"headerRow": o.HeaderRow,
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
	ObjectEntries bool `json:"objectEntries"`
}

func (*JsonOptions) Format() DataFormat {
	return JsonDataFormat
}

func (o *JsonOptions) Map() map[string]interface{} {
	if o == nil {
		return nil
	}
	return map[string]interface{}{
		"objectEntries": o.ObjectEntries,
	}
}
