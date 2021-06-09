package detect

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/compression"
	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/qfs"
)

func TestStructure(t *testing.T) {
	ds := &dataset.Dataset{}
	if err := Structure(ds); !errors.Is(err, dataset.ErrNoBody) {
		t.Errorf("expected dataset without an open body file to return dataset.ErrNoBody, got: %v", err)
	}

	ds = &dataset.Dataset{}
	ds.SetBodyFile(qfs.NewMemfileBytes("animals.csv",
		[]byte("Animal,Sound,Weight\ncat,meow,1.4\ndog,bark,3.7\n")))

	if err := Structure(ds); err != nil {
		t.Error(err)
	}

	expect := &dataset.Structure{
		Format: dataset.CSVDataFormat.String(),
		FormatConfig: map[string]interface{}{
			"headerRow":  true,
			"lazyQuotes": true,
		},
		Schema: mustParseJSONSchema([]byte(`{
			"items":{
				"items":[
					{"title":"animal","type":"string"},
					{"title":"sound","type":"string"},
					{"title":"weight","type":"number"}
				],
				"type":"array"},
				"type":"array"
			}`)),
	}

	if diff := cmp.Diff(expect, ds.Structure); diff != "" {
		t.Errorf("mismatched resulting structure (-want +got):\n%s", diff)
	}

	ds = &dataset.Dataset{
		Structure: &dataset.Structure{},
	}
	ds.SetBodyFile(qfs.NewMemfileBytes("animals.json",
		[]byte(`[{"animal":"cat","sound":"meow","weight: 1.4},{"animal":"dog","sound":"bark","weight":3.7}]`)))

	if err := Structure(ds); err != nil {
		t.Error(err)
	}

	expect = &dataset.Structure{
		Format:       dataset.JSONDataFormat.String(),
		FormatConfig: nil,
		Schema:       mustParseJSONSchema([]byte(`{"type":"array"}`)),
	}

	if diff := cmp.Diff(expect, ds.Structure); diff != "" {
		t.Errorf("mismatched resulting structure (-want +got):\n%s", diff)
	}

	ds = &dataset.Dataset{
		Structure: &dataset.Structure{
			Format: dataset.JSONDataFormat.String(),
		},
	}
	ds.SetBodyFile(qfs.NewMemfileBytes("animals.csv",
		[]byte("Animal,Sound,Weight\ncat,meow,1.4\ndog,bark,3.7\n")))

	if err := Structure(ds); err != nil {
		t.Error(err)
	}

	expect = &dataset.Structure{
		Format:       dataset.JSONDataFormat.String(),
		FormatConfig: nil,
		Schema: mustParseJSONSchema([]byte(`{
			"items":{
				"items":[
					{"title":"animal","type":"string"},
					{"title":"sound","type":"string"},
					{"title":"weight","type":"number"}
				],
				"type":"array"},
				"type":"array"
			}`)),
	}

	if ds.Structure.Format != dataset.JSONDataFormat.String() {
		t.Errorf("format was already set to %s, should not be changed to %s", dataset.JSONDataFormat.String(), ds.Structure.Format)
	}
	if ds.Structure.FormatConfig != nil {
		t.Errorf("format config should not be set when inferred format & input format don't match")
	}
	if diff := cmp.Diff(expect, ds.Structure); diff != "" {
		t.Errorf("mismatched resulting structure (-want +got):\n%s", diff)
	}

	// fully hydrated structure should hit the fast path, change nothing
	if err := Structure(ds); err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(expect, ds.Structure); diff != "" {
		t.Errorf("mismatched resulting structure (-want +got):\n%s", diff)
	}
}

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

			if diff := dstest.CompareStructures(expect, st); diff != "" {
				t.Errorf("case %d structure mismatch (-want +got):\n%s", i, diff)
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

func TestFormatFromFilename(t *testing.T) {
	cases := []struct {
		path       string
		expectFmt  dataset.DataFormat
		expectComp compression.Format
		err        string
	}{
		{"foo/bar/baz.csv", dataset.CSVDataFormat, compression.FmtNone, ""},
		{"foo/bar/baz.json", dataset.JSONDataFormat, compression.FmtNone, ""},
		{"foo/bar/baz.xml", dataset.XMLDataFormat, compression.FmtNone, ""},
		{"foo/bar/baz.xlsx", dataset.XLSXDataFormat, compression.FmtNone, ""},
		{"foo/bar/baz.cbor", dataset.CBORDataFormat, compression.FmtNone, ""},

		{"foo/bar/baz.csv.zst", dataset.CSVDataFormat, compression.FmtZStandard, ""},
		{"foo/bar/baz.json.gzip", dataset.JSONDataFormat, compression.FmtGZip, ""},
		{"foo/bar/baz.xlsx", dataset.XLSXDataFormat, compression.FmtNone, ""},
		{"foo/bar/baz.cbor", dataset.CBORDataFormat, compression.FmtNone, ""},

		{"foo/bar/baz.xml.blarg", dataset.UnknownDataFormat, compression.FmtNone, "unsupported file type: '.blarg'"},
		{"foo/bar/baz", dataset.UnknownDataFormat, compression.FmtNone, "no file extension provided"},
		{"foo/bar/baz.jpg", dataset.UnknownDataFormat, compression.FmtNone, "unsupported file type: '.jpg'"},
	}

	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			gotF, gotComp, err := FormatFromFilename(c.path)
			if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
				t.Fatalf("error expected: %q, got: %q", c.err, err)
			}

			if c.expectFmt != gotF {
				t.Errorf("data format mismatch. expected: %q, got: %q", c.expectFmt, gotF)
			}
			if c.expectComp != gotComp {
				t.Errorf("compression format mismatch. expected: %q, got: %q", c.expectComp, gotComp)
			}
		})
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

func mustParseJSONSchema(data []byte) map[string]interface{} {
	v := map[string]interface{}{}
	if err := json.Unmarshal(data, &v); err != nil {
		panic(err)
	}
	return v
}
