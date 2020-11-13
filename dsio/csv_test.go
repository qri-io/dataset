package dsio

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/dataset/tabular"
)

const csvData = `col_a,col_b,col_c,col_d,col_3,col_f,col_g
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null`

var csvStruct = &dataset.Structure{
	Format: "csv",
	FormatConfig: map[string]interface{}{
		"headerRow": true,
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

var tsvStruct = &dataset.Structure{
	Format: "csv",
	FormatConfig: map[string]interface{}{
		"headerRow":      true,
		"separator":      "\t",
		"lazyQuotes":     true,
		"variadicFields": true,
	},
	Schema: map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type": "array",
			"items": []interface{}{
				map[string]interface{}{"title": "a", "type": "number"},
				map[string]interface{}{"title": "a", "type": "number"},
				map[string]interface{}{"title": "a", "type": "number"},
			},
		},
	},
}

func TestCSVReader(t *testing.T) {
	buf := bytes.NewBuffer([]byte(csvData))
	rdr, err := NewEntryReader(csvStruct, buf)
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
			if len(arr) != 7 {
				t.Errorf("invalid row length for row %d. expected %d, got %d", count, 7, len(arr))
				continue
			}
		} else {
			t.Errorf("expected value to []interface{}. got: %#v", ent.Value)
			continue
		}

		count++
	}
	if count != 5 {
		t.Errorf("expected: %d rows, got: %d", 5, count)
	}
}

func TestBadSchemaCSV(t *testing.T) {
	buf := &bytes.Buffer{}
	st := &dataset.Structure{
		Format: "csv",
		FormatConfig: map[string]interface{}{
			"headerRow": true,
		},
		Schema: map[string]interface{}{"type": "array"},
	}

	_, err := NewEntryReader(st, buf)
	if err == nil {
		t.Fatal("expected error, got nil")
	} else if !errors.Is(err, tabular.ErrInvalidTabularSchema) {
		t.Errorf("expected error to contain an invalid schema error. got: %s", err.Error())
	}

	_, err = NewEntryWriter(st, buf)
	if err == nil {
		t.Fatal("expected error, got nil")
	} else if !errors.Is(err, tabular.ErrInvalidTabularSchema) {
		t.Errorf("expected error to contain an invalid schema error. got: %s", err.Error())
	}
}

func TestCSVReaderLazyQuotes(t *testing.T) {
	data := `number,str
2,"HYDROCHLORIC ACID (1995 AND AFTER "ACID AEROSOLS" ONLY)"`

	st := &dataset.Structure{
		Format: "csv",
		FormatConfig: map[string]interface{}{
			"headerRow":  true,
			"lazyQuotes": true,
		},
		Schema: map[string]interface{}{
			"type": "array",
			"items": map[string]interface{}{
				"type": "array",
				"items": []interface{}{
					map[string]interface{}{"type": "number"},
					map[string]interface{}{"type": "string"},
				},
			},
		},
	}

	rdr, err := NewEntryReader(st, bytes.NewBuffer([]byte(data)))
	if err != nil {
		t.Fatalf("error allocating EntryReader: %s", err.Error())
	}

	_, err = rdr.ReadEntry()
	if err != nil {
		t.Errorf("expected no error: %s", err.Error())
	}
}

func TestTSVReader(t *testing.T) {
	// data separated with tabs, has variadic fields per record, and odd quoting
	// bascially, a trash TSV file that can still parse with lots of CSVOption relaxing
	const oddTSVData = `a	b	c
1	2	""	""""
1
1	2	3	4
`

	buf := bytes.NewBuffer([]byte(oddTSVData))
	rdr, err := NewEntryReader(tsvStruct, buf)
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

		if _, ok := ent.Value.([]interface{}); !ok {
			t.Errorf("expected value to []interface{}. got: %#v", ent.Value)
			continue
		}

		count++
	}
	if count != 3 {
		t.Errorf("expected: %d rows, got: %d", 3, count)
	}
}

func TestCSVWriter(t *testing.T) {
	rows := []Entry{
		// TODO - vary up test input
		{Value: []interface{}{"a", float64(12), 23, nil}},
		{Value: []interface{}{"a", float64(12), 23, []interface{}{"foo", "bar"}}},
		{Value: []interface{}{"a", float64(12), 23, map[string]interface{}{"foo": "bar"}}},
		{Value: []interface{}{"a", float64(12), int64(23), false}},
		{Value: []interface{}{"a", float64(12), 23, false}},
	}

	buf := &bytes.Buffer{}
	rw, err := NewEntryWriter(csvStruct, buf)
	if err != nil {
		t.Errorf("error allocating EntryWriter: %s", err.Error())
		return
	}
	st := rw.Structure()
	if diff := dstest.CompareStructures(st, csvStruct); diff != "" {
		t.Errorf("structure mismatch (-want +got):\n%s", diff)
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
	if bytes.Equal(buf.Bytes(), []byte(csvData)) {
		t.Errorf("output mismatch. %s != %s", buf.String(), csvData)
	}
}

func TestTSVWriter(t *testing.T) {
	rows := []Entry{
		// TODO - vary up test input
		{Value: []interface{}{"a", float64(12), 23, nil}},
		{Value: []interface{}{"a", float64(12), 23, []interface{}{"foo", "bar"}}},
	}

	expect := `a	b	c
a	12	23	
a	12	23	"[""foo"",""bar""]"`

	buf := &bytes.Buffer{}
	rw, err := NewEntryWriter(tsvStruct, buf)
	if err != nil {
		t.Errorf("error allocating EntryWriter: %s", err.Error())
		return
	}
	st := rw.Structure()
	if diff := dstest.CompareStructures(st, tsvStruct); diff != "" {
		t.Errorf("structure mismatch (-want +got):\n%s", diff)
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
	if bytes.Equal(buf.Bytes(), []byte(expect)) {
		t.Errorf("output mismatch. %s != %s", buf.String(), expect)
	}
}
func BenchmarkCSVWriterArrays(b *testing.B) {
	const NumWrites = 1000
	st := &dataset.Structure{Format: "csv", Schema: dataset.BaseSchemaObject}

	for n := 0; n < b.N; n++ {
		buf := &bytes.Buffer{}
		w, _ := NewCSVWriter(st, buf)
		for i := 0; i < NumWrites; i++ {
			// Write an array entry.
			arrayEntry := Entry{Index: i, Value: "test"}
			w.WriteEntry(arrayEntry)
		}
	}
}

func BenchmarkCSVWriterObjects(b *testing.B) {
	const NumWrites = 1000
	st := &dataset.Structure{Format: "csv", Schema: dataset.BaseSchemaObject}

	for n := 0; n < b.N; n++ {
		buf := &bytes.Buffer{}
		w, _ := NewCSVWriter(st, buf)
		for i := 0; i < NumWrites; i++ {
			// Write an object entry.
			objectEntry := Entry{Key: "key", Value: "test"}
			w.WriteEntry(objectEntry)
		}
	}
}

func BenchmarkCSVReader(b *testing.B) {
	st := &dataset.Structure{Format: "csv", Schema: dataset.BaseSchemaArray}

	for n := 0; n < b.N; n++ {
		file, err := os.Open(testdataFile("../dsio/testdata/movies/body.csv"))
		if err != nil {
			b.Errorf("unexpected error: %s", err.Error())
		}
		r, _ := NewCSVReader(st, file)
		for {
			_, err = r.ReadEntry()
			if err != nil {
				break
			}
		}
	}
}
