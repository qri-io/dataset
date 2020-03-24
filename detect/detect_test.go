package detect

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
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

		{"testdata/invalid.cbor", "", "invalid top-level type for CBOR data. cbor datasets must begin with either an array or map"},
		{"testdata/cbor_object.cbor", "testdata/cbor_object.structure.json", ""},
		{"testdata/cbor_array.cbor", "testdata/cbor_array.structure.json", ""},
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
				fmt.Printf("exp: %#v\n", expect.FormatConfig)
				fmt.Printf("got: %#v\n\n", st.FormatConfig)
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
		{"foo/bar/baz.xlsx", dataset.XLSXDataFormat, ""},
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

func TestTabularSchemaFromTabularData(t *testing.T) {
	good := []struct {
		name       string
		input      interface{}
		expectJSON string
	}{
		{"array of arrays, three columns",
			[]interface{}{
				[]interface{}{"one", "two", 3, false, nil},
				[]interface{}{"four", "five", 6, false, nil},
			}, `{
			"type":"array", 
			"items":{
				"type":"array",
				"items": [
					{"title":"col_0","type":"string"},
					{"title":"col_1","type":"string"},
					{"title":"col_2","type":"number"},
					{"title":"col_3","type":"boolean"},
					{"title":"col_4","type":"null"}
			]}
		}`},
	}

	for _, c := range good {
		t.Run(c.name, func(t *testing.T) {
			got, err := TabularSchemaFromTabularData(c.input)
			if err != nil {
				t.Fatal(err)
			}

			expect := map[string]interface{}{}
			if err := json.Unmarshal([]byte(c.expectJSON), &expect); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(expect, got); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
			}
		})
	}

	bad := []struct {
		name  string
		input interface{}
		err   string
	}{
		{"no rows",
			[]interface{}{},
			"invalid tabular data: missing row data",
		},
		{"unsupported inner object",
			[]interface{}{
				map[string]interface{}{
					"foo": "bar",
				},
			},
			"invalid tabular data: array schemas must use an inner array for rows",
		},
		{"unsupported object wrapper",
			map[string]interface{}{},
			"invalid tabular data: cannot interpret object-based tabular schemas",
		},
	}

	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			_, err := TabularSchemaFromTabularData(c.input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, ErrInvalidTabularData) {
				t.Error("expected returned error to be an instance of ErrInvalidTabularData")
			}
			if diff := cmp.Diff(c.err, err.Error()); diff != "" {
				t.Errorf("err string mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
