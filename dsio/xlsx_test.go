package dsio

import (
	"bytes"
	"os"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
)

var xlsxStruct = &dataset.Structure{
	Format: "xlsx",
	FormatConfig: map[string]interface{}{
		"sheetName": "Sheet1",
	},
	Schema: map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type": "array",
			"items": []interface{}{
				map[string]interface{}{"title": "col_a", "type": "string"},
				map[string]interface{}{"title": "col_b", "type": "number"},
				map[string]interface{}{"title": "col_c", "type": "integer"},
				map[string]interface{}{"title": "col_d", "type": "boolean"},
				map[string]interface{}{"title": "col_e", "type": "object"},
				map[string]interface{}{"title": "col_f", "type": "array"},
				map[string]interface{}{"title": "col_g", "type": "null"},
			},
		},
	},
}

func TestXLSXReader(t *testing.T) {
	f, err := os.Open("testdata/xlsx/simple/body.xlsx")
	if err != nil {
		t.Fatal(err.Error())
	}

	rdr, err := NewEntryReader(xlsxStruct, f)
	if err != nil {
		t.Errorf("error allocating EntryReader: %s", err.Error())
		return
	}
	count := 0
	for {
		ent, err := rdr.ReadEntry()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Errorf("unexpected error: %s", err.Error())
			return
		}

		if arr, ok := ent.Value.([]interface{}); ok {
			if len(arr) != 2 {
				t.Errorf("invalid row length for row %d. expected %d, got %d", count, 7, len(arr))
				continue
			}
		} else {
			t.Errorf("expected value to []interface{}. got: %#v", ent.Value)
			continue
		}

		count++
	}
	if count != 4 {
		t.Errorf("expected: %d rows, got: %d", 4, count)
	}
}

func TestColIndexToLetters(t *testing.T) {
	cases := []struct {
		in     int
		expect string
	}{
		{0, "A"},
		{25, "Z"},
		{26, "AA"},
	}
	for i, c := range cases {
		got := ColIndexToLetters(c.in)
		if got != c.expect {
			t.Errorf("case %d expected: %s, got: %s", i, c.expect, got)
		}
	}
}

func TestXLSXWriter(t *testing.T) {
	rows := []Entry{
		// TODO - vary up test input
		{Value: []interface{}{"a", float64(12), 23, nil}},
		{Value: []interface{}{"a", float64(12), 23, []interface{}{"foo", "bar"}}},
		{Value: []interface{}{"a", float64(12), 23, map[string]interface{}{"foo": "bar"}}},
		{Value: []interface{}{"a", float64(12), int64(23), false}},
		{Value: []interface{}{"a", float64(12), 23, false}},
	}

	buf := &bytes.Buffer{}
	rw, err := NewEntryWriter(xlsxStruct, buf)
	if err != nil {
		t.Errorf("error allocating EntryWriter: %s", err.Error())
		return
	}
	st := rw.Structure()
	if diff := dstest.CompareStructures(st, xlsxStruct); diff != "" {
		t.Errorf("structure mismatch: %s", diff)
		return
	}

	for i, row := range rows {
		if err := rw.WriteEntry(row); err != nil {
			t.Errorf("row %d write error: %s", i, err.Error())
		}
	}

	if err := rw.Close(); err != nil {
		t.Errorf("close reader error: %s", err.Error())
		return
	}
}

func TestXLSXCompression(t *testing.T) {
	if _, err := NewXLSXReader(&dataset.Structure{Format: "xlsx", Compression: "gzip"}, nil); err == nil {
		t.Error("expected xlsx to fail when using compression")
	}
	if _, err := NewXLSXWriter(&dataset.Structure{Format: "xlsx", Compression: "gzip"}, nil); err == nil {
		t.Error("expected xlsx to fail when using compression")
	}
}

func BenchmarkXLSXReader(b *testing.B) {
	st := &dataset.Structure{Format: "xlsx", Schema: dataset.BaseSchemaArray}

	for n := 0; n < b.N; n++ {
		file, err := os.Open("testdata/movies/data.xlsx")
		if err != nil {
			b.Errorf("unexpected error: %s", err.Error())
		}
		r, err := NewXLSXReader(st, file)
		if err != nil {
			b.Errorf("unexpected error: %s", err.Error())
		}
		for {
			_, err = r.ReadEntry()
			if err != nil {
				break
			}
		}
	}
}
