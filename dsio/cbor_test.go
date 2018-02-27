package dsio

// import (
// 	"bytes"
// 	"encoding/json"
// 	"os"
// 	"testing"

// 	"github.com/qri-io/dataset"
// 	"github.com/qri-io/dataset/vals"
// 	"github.com/qri-io/jsonschema"
// )

// func TestCBORReader(t *testing.T) {
// 	cases := []struct {
// 		structure *dataset.Structure
// 		filepath  string
// 		count     int
// 		err       string
// 	}{
// 		{&dataset.Structure{}, "testdata/city_data.json", 0, "schema required for JSON reader"},
// 		{&dataset.Structure{Schema: jsonschema.Must(`false`)}, "testdata/city_data.json", 0, "invalid schema for JSON data format. root must be either an array or object type"},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaArray,
// 		},
// 			"testdata/city_data.json", 6, ""},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaObject,
// 		},
// 			"testdata/sitemap_object.json", 7, ""},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaObject,
// 		}, "testdata/links_object.json", 20, ""},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaArray,
// 		}, "testdata/links_array.json", 20, ""},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaArray,
// 		}, "testdata/json_array.json", 10, ""},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaObject,
// 		}, "testdata/json_object.json", 10, ""},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaArray,
// 		}, "testdata/craigslist.json", 1200, ""},
// 		{&dataset.Structure{
// 			Format: dataset.JSONDataFormat,
// 			Schema: dataset.BaseSchemaObject,
// 		}, "testdata/sitemap.json", 1, ""},
// 	}

// 	for i, c := range cases {
// 		f, err := os.Open(c.filepath)
// 		if err != nil {
// 			t.Errorf("case %d error opening data file: %s", i, err.Error())
// 			continue
// 		}

// 		r, err := NewCBORReader(c.structure, f)
// 		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
// 			t.Errorf("case %d error mismatch. expected: %s. got: %s", i, c.err, err)
// 			continue
// 		} else if c.err != "" {
// 			continue
// 		}

// 		if r.Structure() == nil {
// 			t.Errorf("nil structure?")
// 			return
// 		}

// 		j := 0
// 		vs := []vals.Value{}
// 		for {
// 			// TODO - inspect row output for well formed json
// 			v, err := r.ReadValue()
// 			if err != nil {
// 				if err.Error() == "EOF" {
// 					break
// 				}
// 				t.Errorf("case %d error reading row %d: %s", i, j, err.Error())
// 				break
// 			}
// 			vs = append(vs, v)
// 			j++
// 		}

// 		if c.count != j {
// 			t.Errorf("case %d count mismatch. expected: %d, got: %d", i, c.count, j)
// 			t.Log(vs)
// 			continue
// 		}

// 		// for _, ent := range c.entries {
// 		//  if err := r.ReadRow(ent); err != nil {
// 		//    t.Errorf("case %d WriteRow error: %s", i, err.Error())
// 		//    break
// 		//  }
// 		// }
// 		// if err := w.Close(); err != nil {
// 		//  t.Errorf("case %d Close error: %s", i, err.Error())
// 		// }

// 		// if string(buf.Bytes()) != c.out {
// 		//  t.Errorf("case %d result mismatch. expected:\n%s\ngot:\n%s", i, c.out, string(buf.Bytes()))
// 		// }

// 		// var v interface{}
// 		// if cfg, ok := c.structure.FormatConfig.(*dataset.JSONOptions); ok && cfg.ArrayEntries {
// 		//  v = []interface{}{}
// 		// } else {
// 		//  v = map[string]interface{}{}
// 		// }

// 		// if err := json.Unmarshal(buf.Bytes(), &v); err != nil {
// 		//  t.Errorf("unmarshal error: %s", err.Error())
// 		// }
// 	}
// }

// func TestCBORWriter(t *testing.T) {
// 	objst := &dataset.Structure{Schema: dataset.BaseSchemaObject}
// 	arrst := &dataset.Structure{Schema: dataset.BaseSchemaArray}

// 	cases := []struct {
// 		structure *dataset.Structure
// 		entries   vals.Array
// 		out       string
// 		err       string
// 	}{
// 		{&dataset.Structure{}, vals.Array{}, "[]", "schema required for JSON writer"},
// 		{&dataset.Structure{Schema: jsonschema.Must(`true`)}, vals.Array{}, "[]", "invalid schema for JSON data format. root must be either an array or object type"},

// 		{arrst, vals.Array{}, "[]", ""},
// 		{objst, vals.Array{}, "{}", ""},
// 		{objst, vals.Array{vals.ObjectValue{"a", vals.String("hello")}, vals.ObjectValue{"b", vals.String("world")}}, `{"a":"hello","b":"world"}`, ""},
// 		{objst, vals.Array{vals.ObjectValue{"a", vals.String("hello")}, vals.ObjectValue{"b", vals.String("world")}}, `{"a":"hello","b":"world"}`, ""},
// 	}

// 	for i, c := range cases {
// 		buf := &bytes.Buffer{}
// 		w, err := NewCBORWriter(c.structure, buf)
// 		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
// 			t.Errorf("case %d error mismatch. expected: %s. got: %s", i, c.err, err)
// 			continue
// 		} else if c.err != "" {
// 			continue
// 		}

// 		for _, ent := range c.entries {
// 			if err := w.WriteValue(ent); err != nil {
// 				t.Errorf("case %d WriteValue error: %s", i, err.Error())
// 				break
// 			}
// 		}
// 		if err := w.Close(); err != nil {
// 			t.Errorf("case %d Close error: %s", i, err.Error())
// 		}

// 		if string(buf.Bytes()) != c.out {
// 			t.Errorf("case %d result mismatch. expected:\n%s\ngot:\n%s", i, c.out, string(buf.Bytes()))
// 		}

// 		var v interface{}
// 		if w.ContainerType() == "object" {
// 			v = []interface{}{}
// 		} else {
// 			v = map[string]interface{}{}
// 		}

// 		if err := json.Unmarshal(buf.Bytes(), &v); err != nil {
// 			t.Errorf("unmarshal error: %s", err.Error())
// 		}
// 	}
// }

// func TestCBORWriterNonObjectValue(t *testing.T) {
// 	buf := &bytes.Buffer{}
// 	w, err := NewCBORWriter(&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}, buf)
// 	if err != nil {
// 		t.Errorf("unexpected error creating writer: %s", err.Error())
// 		return
// 	}

// 	err = w.WriteValue(vals.Boolean(false))
// 	expect := `only vals.ObjectValue can be written to a JSON object writer`
// 	if err.Error() != expect {
// 		t.Errorf("error mismatch. expected: %s. got: %s", expect, err.Error())
// 		return
// 	}
// }

// func TestCBORWriterDoubleKey(t *testing.T) {
// 	buf := &bytes.Buffer{}
// 	w, err := NewCBORWriter(&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}, buf)
// 	if err != nil {
// 		t.Errorf("unexpected error creating writer: %s", err.Error())
// 		return
// 	}

// 	if w.Structure() == nil {
// 		t.Errorf("nil structure?")
// 	}

// 	if err := w.WriteValue(vals.ObjectValue{"a", vals.String("foo")}); err != nil {
// 		t.Errorf("unexpected error writing key: %s", err.Error())
// 		return
// 	}

// 	err = w.WriteValue(vals.ObjectValue{"a", vals.Boolean(true)})
// 	if err == nil {
// 		t.Errorf("expected an error on second write with duplicate key")
// 		return
// 	}

// 	expect := `key already written: "a"`
// 	if err.Error() != expect {
// 		t.Errorf("error mismatch. expected: %s. got: %s", expect, err.Error())
// 		return
// 	}
// }
