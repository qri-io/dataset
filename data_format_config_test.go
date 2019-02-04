package dataset

import (
	"fmt"
	"testing"
)

func CompareFormatConfigs(a, b FormatConfig) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("FormatConfig mismatch: %s != %s", a, b)
	}

	if a.Format() != b.Format() {
		return fmt.Errorf("FormatConfig mistmatch %s != %s", a.Format(), b.Format())
	}

	// TODO - exhaustive check

	return nil
}

func TestParseFormatConfigMap(t *testing.T) {
	cases := []struct {
		df   DataFormat
		opts map[string]interface{}
		cfg  FormatConfig
		err  string
	}{
		{CSVDataFormat, map[string]interface{}{}, &CSVOptions{}, ""},
		{JSONDataFormat, map[string]interface{}{}, &JSONOptions{}, ""},
		{XLSXDataFormat, map[string]interface{}{}, &XLSXOptions{}, ""},
	}

	for i, c := range cases {
		cfg, err := ParseFormatConfigMap(c.df, c.opts)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch: %s != %s", i, c.err, err)
			continue
		}
		if err := CompareFormatConfigs(c.cfg, cfg); err != nil {
			t.Errorf("case %d config err: %s", i, err.Error())
			continue
		}
	}
}

func TestNewCSVOptions(t *testing.T) {
	cases := []struct {
		opts map[string]interface{}
		res  *CSVOptions
		err  string
	}{
		{nil, &CSVOptions{}, ""},
		{map[string]interface{}{}, &CSVOptions{}, ""},
		{map[string]interface{}{"headerRow": true}, &CSVOptions{HeaderRow: true}, ""},
		{map[string]interface{}{"headerRow": "foo"}, nil, "invalid headerRow value: foo"},
		{map[string]interface{}{"lazyQuotes": true}, &CSVOptions{LazyQuotes: true}, ""},
		{map[string]interface{}{"lazyQuotes": "foo"}, nil, "invalid lazyQuotes value: foo"},
		{map[string]interface{}{"separator": "\t"}, &CSVOptions{Separator: '\t'}, ""},
		{map[string]interface{}{"separator": "\t\t"}, nil, "separator must be a single character"},
		{map[string]interface{}{"separator": true}, nil, "invalid separator value: true"},
		{map[string]interface{}{"variadicFields": true}, &CSVOptions{VariadicFields: true}, ""},
		{map[string]interface{}{"variadicFields": "foo"}, nil, "invalid variadicFields value: foo"},
	}

	for i, c := range cases {
		got, err := NewCSVOptions(c.opts)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if c.err == "" {
			if got.HeaderRow != c.res.HeaderRow {
				t.Errorf("case %d HeaderRow expected: %t, got: %t", i, got.HeaderRow, c.res.HeaderRow)
				continue
			}
		}
	}
}

func TestCSVOptionsMap(t *testing.T) {
	cases := []struct {
		opt *CSVOptions
		res map[string]interface{}
	}{
		{nil, nil},
		{&CSVOptions{HeaderRow: true}, map[string]interface{}{"headerRow": true}},
	}

	for i, c := range cases {
		got := c.opt.Map()
		for key, val := range c.res {
			if got[key] != val {
				t.Errorf("case %d, key '%s' expected: '%s' got:'%s'", i, key, val, got[key])
			}
		}
	}
}

func TestNewJSONOptions(t *testing.T) {
	cases := []struct {
		opts map[string]interface{}
		res  *JSONOptions
		err  string
	}{
		{nil, &JSONOptions{}, ""},
		{map[string]interface{}{}, &JSONOptions{}, ""},
	}

	for i, c := range cases {
		_, err := NewJSONOptions(c.opts)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error expected: '%s', got: '%s'", i, c.err, err.Error())
			continue
		}
	}
}

func TestJSONOptionsMap(t *testing.T) {
	cases := []struct {
		opt *JSONOptions
		res map[string]interface{}
	}{
		{nil, nil},
		{&JSONOptions{}, map[string]interface{}{}},
	}

	for i, c := range cases {
		got := c.opt.Map()
		for key, val := range c.res {
			if got[key] != val {
				t.Errorf("case %d, key '%s' expected: '%s' got:'%s'", i, key, val, got[key])
			}
		}
	}
}

func TestNewXLSXOptions(t *testing.T) {
	cases := []struct {
		opts map[string]interface{}
		res  *XLSXOptions
		err  string
	}{
		{nil, &XLSXOptions{}, ""},
		{map[string]interface{}{}, &XLSXOptions{}, ""},
		{map[string]interface{}{"sheetName": "foo"}, &XLSXOptions{SheetName: "foo"}, ""},
		{map[string]interface{}{"sheetName": true}, nil, "invalid sheetName value: true"},
	}

	for i, c := range cases {
		got, err := NewXLSXOptions(c.opts)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if c.err == "" {
			xlsxo, ok := got.(*XLSXOptions)
			if !ok {
				t.Errorf("case %d didn't return a CSVOptions pointer", i)
				continue
			}

			if xlsxo.SheetName != c.res.SheetName {
				t.Errorf("case %d SheetName expected: %s, got: %s", i, xlsxo.SheetName, c.res.SheetName)
				continue
			}
		}
	}
}

func TestXLSXOptionsMap(t *testing.T) {
	cases := []struct {
		opt *XLSXOptions
		res map[string]interface{}
	}{
		{nil, nil},
		{&XLSXOptions{}, map[string]interface{}{}},
		{&XLSXOptions{SheetName: "foo"}, map[string]interface{}{"sheetName": "foo"}},
	}

	for i, c := range cases {
		got := c.opt.Map()
		for key, val := range c.res {
			if got[key] != val {
				t.Errorf("case %d, key '%s' expected: '%s' got:'%s'", i, key, val, got[key])
			}
		}
	}
}
