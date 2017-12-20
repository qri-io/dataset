package detect

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/qri-io/dataset"
)

func TestFromFile(t *testing.T) {
	cases := []struct {
		inpath, dspath string
		err            string
	}{
		{"not/a/file.csv", "", "open not/a/file.csv: no such file or directory"},
		{"testdata/hours-with-header.csv", "testdata/hours-with-header.resource.json", ""},
		{"testdata/hours.csv", "testdata/hours.resource.json", ""},
		{"testdata/spelling.csv", "testdata/spelling.resource.json", ""},
		{"testdata/daily_wind_2011.csv", "testdata/daily_wind_2011.resource.json", ""},
	}

	for i, c := range cases {
		ds, err := FromFile(c.inpath)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if c.dspath != "" {
			data, err := ioutil.ReadFile(c.dspath)
			if err != nil {
				t.Error(err)
				continue
			}
			expect := &dataset.Structure{}
			if err := json.Unmarshal(data, expect); err != nil {
				t.Error(err)
				continue
			}

			// if ds.Name != expect.Name {
			// 	t.Errorf("case %d name mismatch. expected '%s', got '%s'", i, expect.Name, ds.Name)
			// }

			if expect.Format != ds.Format {
				t.Errorf("case %d format mismatch. expected '%s', got '%s'", i, expect.Format, ds.Format)
			}

			// if expect.File != ds.File {
			// 	t.Errorf("case %d file mismatch. expected '%s', got '%s'", i, expect.File, ds.File)
			// }

			if len(expect.Schema.Fields) != len(ds.Schema.Fields) {
				t.Errorf("case %d field length mismatch. expected: %d, got: %d", i, len(expect.Schema.Fields), len(ds.Schema.Fields))
				continue
			}

			for j, f := range expect.Schema.Fields {
				if f.Type != ds.Schema.Fields[j].Type {
					t.Errorf("case %d field %d:%s type mismatch. expected: %s, got: %s", i, j, f.Name, f.Type, ds.Schema.Fields[j].Type)
				}
				if f.Name != ds.Schema.Fields[j].Name {
					t.Errorf("case %d field %d name mismatch. expected: %s, got: %s", i, j, f.Name, ds.Schema.Fields[j].Name)
				}
			}
		}
	}
}

func TestReplaceSoloCarriageReturns(t *testing.T) {
	input := []byte("foo\r\rbar\r\nbaz\r\r")
	expect := []byte("foo\r\n\r\nbar\r\nbaz\r\n\r\n")

	got := ReplaceSoloCarriageReturns(input)
	if !bytes.Equal(expect, got) {
		t.Errorf("byte mismatch. expected:\n%v\ngot:\n%v", expect, got)
	}
}

func TestExtensionDataFormat(t *testing.T) {
	cases := []struct {
		path   string
		expect dataset.DataFormat
		err    string
	}{
		{"foo/bar/baz.csv", dataset.CSVDataFormat, ""},
		{"foo/bar/baz.json", dataset.JSONDataFormat, ""},
		{"foo/bar/baz.xml", dataset.XMLDataFormat, ""},
		{"foo/bar/baz.xls", dataset.XLSDataFormat, ""},
		{"foo/bar/baz", dataset.UnknownDataFormat, "no file extension provided"},
		{"foo/bar/baz.jpg", dataset.UnknownDataFormat, "unsupported file type: '.jpg'"},
	}

	for i, c := range cases {
		got, err := ExtensionDataFormat(c.path)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if got != c.expect {
			t.Errorf("case %d datatype mismatch. expected: '%s', got: '%s'", i, c.expect, got)
			continue
		}
	}
}
