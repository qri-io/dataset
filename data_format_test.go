package dataset

import (
	"bytes"
	"testing"
)

func TestSupportedDataFormats(t *testing.T) {
	expect := []DataFormat{
		CBORDataFormat,
		JSONDataFormat,
		CSVDataFormat,
		XLSXDataFormat,
		NDJSONDataFormat,
	}

	for i, f := range SupportedDataFormats() {
		if expect[i] != f {
			t.Errorf("index %d mismatch. expected: %s got: %s", i, expect, f)
		}
	}
}

func TestDataFormatString(t *testing.T) {
	cases := []struct {
		f      DataFormat
		expect string
	}{
		{UnknownDataFormat, ""},
		{CSVDataFormat, "csv"},
		{JSONDataFormat, "json"},
		{XMLDataFormat, "xml"},
		{XLSXDataFormat, "xlsx"},
		{CBORDataFormat, "cbor"},
		{NDJSONDataFormat, "ndjson"},
	}

	for i, c := range cases {
		if got := c.f.String(); got != c.expect {
			t.Errorf("case %d mismatch. expected: %q, got: %q", i, c.expect, got)
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
		{".csv", CSVDataFormat, ""},
		{"csv", CSVDataFormat, ""},
		{".json", JSONDataFormat, ""},
		{"json", JSONDataFormat, ""},
		{".xml", XMLDataFormat, ""},
		{"xml", XMLDataFormat, ""},
		{".xlsx", XLSXDataFormat, ""},
		{"xlsx", XLSXDataFormat, ""},
		{"cbor", CBORDataFormat, ""},
		{".cbor", CBORDataFormat, ""},
		{".ndjson", NDJSONDataFormat, ""},
		{"ndjson", NDJSONDataFormat, ""},
		{".jsonl", NDJSONDataFormat, ""},
		{"jsonl", NDJSONDataFormat, ""},
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
	cases := []struct {
		format DataFormat
		expect []byte
		err    string
	}{
		{CSVDataFormat, []byte(`"csv"`), ""},
		{JSONDataFormat, []byte(`"json"`), ""},
		{XMLDataFormat, []byte(`"xml"`), ""},
		{XLSXDataFormat, []byte(`"xlsx"`), ""},
		{CBORDataFormat, []byte(`"cbor"`), ""},
		{NDJSONDataFormat, []byte(`"ndjson"`), ""},
	}
	for i, c := range cases {
		got, err := c.format.MarshalJSON()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if !bytes.Equal(got, c.expect) {
			t.Errorf(`case %d response mismatch. expected: %s, got: %s`, i, string(c.expect), string(got))
			continue
		}
	}
}

func TestDataFormatUnmarshalJSON(t *testing.T) {
	cases := []struct {
		data   []byte
		expect DataFormat
		err    string
	}{
		{[]byte(`"csv"`), CSVDataFormat, ""},
		{[]byte(`"json"`), JSONDataFormat, ""},
		{[]byte(`"xml"`), XMLDataFormat, ""},
		{[]byte(`"xlsx"`), XLSXDataFormat, ""},
		{[]byte(`"cbor"`), CBORDataFormat, ""},
		{[]byte(`"ndjson"`), NDJSONDataFormat, ""},
	}

	for i, c := range cases {
		a := DataFormat(0)
		got := &a
		err := got.UnmarshalJSON(c.data)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if *got != c.expect {
			t.Errorf(`case %d response mismatch. expected: %s, got: %s`, i, c.expect, *got)
			continue
		}

	}
}
