package dsio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/jsonschema"
)

func TestJSONReader(t *testing.T) {
	cases := []struct {
		name      string
		structure *dataset.Structure
		count     int
		err       string
	}{
		{"city", &dataset.Structure{}, 0, "schema required for JSON reader"},
		{"city", &dataset.Structure{Schema: jsonschema.Must(`false`)}, 0, "invalid schema. root must be either an array or object type"},
		{"city", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 6, ""},
		{"sitemap_object", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaObject,
		}, 7, ""},
		{"links_object", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaObject,
		}, 20, ""},
		{"links_array", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 20, ""},
		{"array", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 10, ""},
		{"object", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaObject,
		}, 10, ""},
		{"craigslist", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 1200, ""},
		{"sitemap", &dataset.Structure{
			Format: dataset.JSONDataFormat,
			Schema: dataset.BaseSchemaObject,
		}, 1, ""},
	}

	for i, c := range cases {
		tc, err := dstest.NewTestCaseFromDir(fmt.Sprintf("testdata/json/%s", c.name))
		if err != nil {
			t.Errorf("case %d:%s error reading test case: %s", i, c.name, err.Error())
			continue
		}

		r, err := NewJSONReader(c.structure, tc.DataFile())
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d:%s error mismatch. expected: %s. got: %s", i, c.name, c.err, err)
			continue
		} else if c.err != "" {
			continue
		}

		if r.Structure() == nil {
			t.Errorf("nil structure?")
			return
		}

		j := 0
		vs := []Entry{}
		for {
			// TODO - inspect row output for well formed json
			ent, err := r.ReadEntry()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				t.Errorf("case %d error reading row %d: %s", i, j, err.Error())
				break
			}
			vs = append(vs, ent)
			j++
		}

		if c.count != j {
			t.Errorf("case %d count mismatch. expected: %d, got: %d", i, c.count, j)
			t.Log(vs)
			continue
		}

		// for _, ent := range c.entries {
		// 	if err := r.ReadRow(ent); err != nil {
		// 		t.Errorf("case %d WriteRow error: %s", i, err.Error())
		// 		break
		// 	}
		// }
		// if err := w.Close(); err != nil {
		// 	t.Errorf("case %d Close error: %s", i, err.Error())
		// }

		// if string(buf.Bytes()) != c.out {
		// 	t.Errorf("case %d result mismatch. expected:\n%s\ngot:\n%s", i, c.out, string(buf.Bytes()))
		// }

		// var v interface{}
		// if cfg, ok := c.structure.FormatConfig.(*dataset.JSONOptions); ok && cfg.ArrayEntries {
		// 	v = []interface{}{}
		// } else {
		// 	v = map[string]interface{}{}
		// }

		// if err := json.Unmarshal(buf.Bytes(), &v); err != nil {
		// 	t.Errorf("unmarshal error: %s", err.Error())
		// }
	}
}

func TestJSONWriter(t *testing.T) {
	objst := &dataset.Structure{Schema: dataset.BaseSchemaObject}
	arrst := &dataset.Structure{Schema: dataset.BaseSchemaArray}

	cases := []struct {
		structure *dataset.Structure
		entries   []Entry
		out       string
		err       string
	}{
		{&dataset.Structure{}, []Entry{}, "[]", "schema required for JSON writer"},
		{&dataset.Structure{Schema: jsonschema.Must(`true`)}, []Entry{}, "[]", "invalid schema. root must be either an array or object type"},

		{arrst, []Entry{}, "[]", ""},
		{objst, []Entry{}, "{}", ""},
		{objst, []Entry{{Key: "a", Value: "hello"}, {Key: "b", Value: "world"}}, `{"a":"hello","b":"world"}`, ""},
		{objst, []Entry{{Key: "a", Value: "hello"}, {Key: "b", Value: "world"}}, `{"a":"hello","b":"world"}`, ""},
	}

	for i, c := range cases {
		buf := &bytes.Buffer{}
		w, err := NewJSONWriter(c.structure, buf)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s. got: %s", i, c.err, err)
			continue
		} else if c.err != "" {
			continue
		}

		for _, ent := range c.entries {
			if err := w.WriteEntry(ent); err != nil {
				t.Errorf("case %d WriteEntry error: %s", i, err.Error())
				break
			}
		}
		if err := w.Close(); err != nil {
			t.Errorf("case %d Close error: %s", i, err.Error())
		}

		if string(buf.Bytes()) != c.out {
			t.Errorf("case %d result mismatch. expected:\n%s\ngot:\n%s", i, c.out, string(buf.Bytes()))
		}

		var v interface{}
		if w.ContainerType() == "object" {
			v = []interface{}{}
		} else {
			v = map[string]interface{}{}
		}

		if err := json.Unmarshal(buf.Bytes(), &v); err != nil {
			t.Errorf("unmarshal error: %s", err.Error())
		}
	}
}

func TestJSONWriterNonObjectEntry(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewJSONWriter(&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}, buf)
	if err != nil {
		t.Errorf("unexpected error creating writer: %s", err.Error())
		return
	}

	err = w.WriteEntry(Entry{Value: false})
	expect := `entry key cannot be empty`
	if err.Error() != expect {
		t.Errorf("error mismatch. expected: %s. got: %s", expect, err.Error())
		return
	}
}

func TestJSONWriterDoubleKey(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewJSONWriter(&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}, buf)
	if err != nil {
		t.Errorf("unexpected error creating writer: %s", err.Error())
		return
	}

	if w.Structure() == nil {
		t.Errorf("nil structure?")
	}

	if err := w.WriteEntry(Entry{Key: "a", Value: "foo"}); err != nil {
		t.Errorf("unexpected error writing key: %s", err.Error())
		return
	}

	err = w.WriteEntry(Entry{Key: "a", Value: true})
	if err == nil {
		t.Errorf("expected an error on second write with duplicate key")
		return
	}

	expect := `key already written: "a"`
	if err.Error() != expect {
		t.Errorf("error mismatch. expected: %s. got: %s", expect, err.Error())
		return
	}
}

func BenchmarkJSONWriterArrays(b *testing.B) {
	const NumWrites = 1000
	st := &dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}

	for n := 0; n < b.N; n++ {
		buf := &bytes.Buffer{}
		w, err := NewJSONWriter(st, buf)
		if err != nil {
			b.Errorf("unexpected error creating writer: %s", err.Error())
			return
		}

		for i := 0; i < NumWrites; i++ {
			// Write an array entry.
			arrayEntry := Entry{Index: i, Value: "test"}
			w.WriteEntry(arrayEntry)
		}
	}
}

func BenchmarkJSONWriterObjects(b *testing.B) {
	const NumWrites = 1000
	st := &dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}

	for n := 0; n < b.N; n++ {
		buf := &bytes.Buffer{}
		w, err := NewJSONWriter(st, buf)
		if err != nil {
			b.Errorf("unexpected error creating writer: %s", err.Error())
			return
		}

		for i := 0; i < NumWrites; i++ {
			// Write an object entry.
			objectEntry := Entry{Key: "key", Value: "test"}
			w.WriteEntry(objectEntry)
		}
	}
}
