package dataset

import (
	"bytes"
	"testing"
)

func TestDataFormatString(t *testing.T) {
	cases := []struct {
		f      DataFormat
		expect string
	}{
		{UnknownDataFormat, ""},
		{CsvDataFormat, "csv"},
		{JsonDataFormat, "json"},
		{XmlDataFormat, "xml"},
		{XlsDataFormat, "xls"},
		{CdxjDataFormat, "cdxj"},
	}

	for i, c := range cases {
		if got := c.f.String(); got != c.expect {
			t.Errorf("case %d mismatch. expected: %s, got: %s", i, c.expect, got)
			continue
		}
	}
}

func TestParseDataFormatString(t *testing.T) {
	cases := []struct {
		in     string
		expect DataFormat
		err    string
	}{
		{"", UnknownDataFormat, ""},
		{".csv", CsvDataFormat, ""},
		{"csv", CsvDataFormat, ""},
		{".json", JsonDataFormat, ""},
		{"json", JsonDataFormat, ""},
		{".xml", XmlDataFormat, ""},
		{"xml", XmlDataFormat, ""},
		{".xls", XlsDataFormat, ""},
		{"xls", XlsDataFormat, ""},
		{".cdxj", CdxjDataFormat, ""},
		{"cdxj", CdxjDataFormat, ""},
	}

	for i, c := range cases {
		got, err := ParseDataFormatString(c.in)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch '%s' != '%s'", i, c.expect, err)
			continue
		}
		if got != c.expect {
			t.Errorf("case %d response mismatch. expected: %s got: %s", i, c.expect, got)
			continue
		}
	}
}

func TestDataFormatMarshalJSON(t *testing.T) {
	got, err := CsvDataFormat.MarshalJSON()
	if err != nil {
		t.Error(err)
		return
	}
	if !bytes.Equal(got, []byte(`"csv"`)) {
		t.Errorf(`expected CsvDataFormat.MarshalJSON to equal "csv"`)
		return
	}
}

func TestDataFormatUnmarshalJSON(t *testing.T) {
	a := DataFormat(0)
	f := &a
	err := f.UnmarshalJSON([]byte(`"json"`))
	if err != nil {
		t.Error(err)
		return
	}
	if *f != JsonDataFormat {
		t.Errorf("expected json format")
	}
}
