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
		{"testdata/hours-with-header.csv", "testdata/hours-with-header.structure.json", ""},
		{"testdata/hours.csv", "testdata/hours.structure.json", ""},
		{"testdata/spelling.csv", "testdata/spelling.structure.json", ""},
		{"testdata/daily_wind_2011.csv", "testdata/daily_wind_2011.structure.json", ""},
		{"testdata/sitemap_array.json", "testdata/sitemap_array.structure.json", ""},
		{"testdata/sitemap_object.json", "testdata/sitemap_object.structure.json", ""},
		{"testdata/array.json", "testdata/sitemap_array.structure.json", ""},
		{"testdata/object.json", "testdata/sitemap_object.structure.json", ""},
	}

	for i, c := range cases {
		st, err := FromFile(c.inpath)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if c.dspath != "" {
			data, err := ioutil.ReadFile(c.dspath)
			if err != nil {
				t.Errorf("case %d: %s", i, err)
				continue
			}
			expect := &dataset.Structure{}
			if err := json.Unmarshal(data, expect); err != nil {
				t.Errorf("case %d: %s", i, err)
				continue
			}

			if err := dataset.CompareStructures(expect, st); err != nil {
				t.Errorf("case %d structure mismatch: %s", i, err.Error())
				continue
			}

			ej, err := json.Marshal(expect.Schema)
			if err != nil {
				t.Errorf("case %d error marshaling expected schema to json: %s", i, err.Error())
				continue
			}

			schj, err := json.Marshal(st.Schema)
			if err != nil {
				t.Errorf("case %d error marshaling expected schema to json: %s", i, err.Error())
				continue
			}

			if !bytes.Equal(ej, schj) {
				t.Errorf("case %d schema mismatch: %s != %s", i, string(ej), string(schj))
				continue
			}
		}
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
		{"foo/bar/baz.cbor", dataset.CBORDataFormat, ""},
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
