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
		{JSONDataFormat, map[string]interface{}{"arrayEntries": true}, &JSONOptions{ArrayEntries: true}, ""},
		{XLSDataFormat, map[string]interface{}{}, nil, "cannot parse configuration for format: xls"},
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
	}

	for i, c := range cases {
		got, err := NewCSVOptions(c.opts)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error expected: '%s', got: '%s'", i, c.err, err.Error())
			continue
		}
		if c.err == "" {
			csvo, ok := got.(*CSVOptions)
			if !ok {
				t.Errorf("case %d didn't return a CSVOptions pointer", i)
				continue
			}

			if csvo.HeaderRow != c.res.HeaderRow {
				fmt.Errorf("case %d HeaderRow expected: %t, got: %t", i, csvo.HeaderRow, c.res.HeaderRow)
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
				t.Errorf("case %s, key '%s' expected: '%s' got:'%s'", i, key, val, got[key])
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
		{map[string]interface{}{"arrayEntries": true}, &JSONOptions{ArrayEntries: true}, ""},
		{map[string]interface{}{"arrayEntries": "foo"}, nil, "invalid arrayEntries value: foo"},
	}

	for i, c := range cases {
		got, err := NewJSONOptions(c.opts)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error expected: '%s', got: '%s'", i, c.err, err.Error())
			continue
		}
		if c.err == "" {
			jsono, ok := got.(*JSONOptions)
			if !ok {
				t.Errorf("case %d didn't return a JSONOptions pointer", i)
				continue
			}

			if jsono.ArrayEntries != c.res.ArrayEntries {
				fmt.Errorf("case %d ArrayEntries expected: %t, got: %t", i, jsono.ArrayEntries, c.res.ArrayEntries)
				continue
			}
		}
	}
}

func TestJSONOptionsMap(t *testing.T) {
	cases := []struct {
		opt *JSONOptions
		res map[string]interface{}
	}{
		{nil, nil},
		{&JSONOptions{ArrayEntries: true}, map[string]interface{}{"arrayEntries": true}},
	}

	for i, c := range cases {
		got := c.opt.Map()
		for key, val := range c.res {
			if got[key] != val {
				t.Errorf("case %s, key '%s' expected: '%s' got:'%s'", i, key, val, got[key])
			}
		}
	}
}
