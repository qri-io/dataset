package dsio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
)

func TestJSONReader(t *testing.T) {
	cases := []struct {
		name      string
		structure *dataset.Structure
		count     int
		err       string
	}{
		{"city", &dataset.Structure{}, 0, "schema required for JSON reader"},
		{"city", &dataset.Structure{Schema: map[string]interface{}{"type": "number"}}, 0, "invalid schema. root must be either an array or object type"},
		{"city", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 6, ""},
		{"sitemap_object", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 7, ""},
		{"links_object", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 20, ""},
		{"links_array", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 20, ""},
		{"array", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 10, ""},
		{"object", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 10, ""},
		{"craigslist", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 1200, ""},
		{"sitemap", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 1, ""},
	}

	for i, c := range cases {
		tc, err := dstest.NewTestCaseFromDir(fmt.Sprintf("testdata/json/%s", c.name))
		if err != nil {
			t.Errorf("case %d:%s error reading test case: %s", i, c.name, err.Error())
			continue
		}

		r, err := NewJSONReader(c.structure, tc.BodyFile())
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
	}
}

func TestJSONReaderBasicParsing(t *testing.T) {
	objSt := &dataset.Structure{
		Format: "json",
		Schema: dataset.BaseSchemaObject,
	}

	cases := []struct {
		text      string
		structure *dataset.Structure
		expect    interface{}
	}{
		{`{"a":1}`, objSt, 1},
		{`{"a": 1}`, objSt, 1},
		{`{"a":"abc"}`, objSt, "abc"},
		{`{"a":4.56}`, objSt, 4.56},
		{`{"a":""}`, objSt, ""},
		{`{"a":null}`, objSt, nil},
		{`{"a":true}`, objSt, true},
		{`{"a":false}`, objSt, false},
		{"{\"a\":\"\xe7\x8a\xac\"}", objSt, "\xe7\x8a\xac"},
		{"{\"a\":\"say \\\"dog\\\"\"}", objSt, "say \"dog\""},
		{"{\"a\":\"say \\\"\\u72ac\\\"\"}", objSt, "say \"\xe7\x8a\xac\""},
		{"{\n  \"a\" : \"b\" }", objSt, "b"},
		{`{"a": "\/"}`, objSt, "/"},
	}

	for i, c := range cases {
		r, _ := NewJSONReader(c.structure, strings.NewReader(c.text))
		ent, err := r.ReadEntry()
		if err != nil {
			t.Errorf("case %d error: %s", i, err)
		}
		if ent.Value != c.expect {
			t.Errorf("case %d value mismatch: %v <> %v", i, ent.Value, c.expect)
		}
	}
}

func TestJSONReaderSmallerBufferForHugeToken(t *testing.T) {
	cases := []struct {
		name      string
		structure *dataset.Structure
		count     int
		err       string
	}{
		{"craigslist", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 1200, ""},
	}

	for i, c := range cases {
		tc, err := dstest.NewTestCaseFromDir(fmt.Sprintf("testdata/json/%s", c.name))
		if err != nil {
			t.Errorf("case %d:%s error reading test case: %s", i, c.name, err.Error())
			continue
		}

		r, err := NewJSONReaderSize(c.structure, tc.BodyFile(), 4096)
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
	}
}

func TestJSONSizeReader(t *testing.T) {
	cases := []struct {
		structure *dataset.Structure
		size      int
		data      string
	}{
		{&dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 16, `[["a","b","cdef"]]`},
		{&dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 16, `[[12345,67890,12345,67890]]`},
		{&dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 18, `[{"a":"b","c":"d","e":"f"}]`},
		{&dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 16, `[[  "a"  ,  "b"  ,  "c"  ,  "d"  ]]`},
		{&dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 16, `[[false, false, false , false]]`},
		{&dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 16, `[[true, true, true, true]]`},
	}

	for i, c := range cases {
		r, err := NewJSONReaderSize(c.structure, strings.NewReader(c.data), c.size)
		if err != nil {
			t.Errorf("case %d unexpected error creating reader: %s", i, err.Error())
			continue
		}

		err = EachEntry(r, func(i int, ent Entry, e error) error {
			if e != nil {
				return e
			}
			return nil
		})

		if err != nil {
			t.Errorf("case %d: unexpected error: %s", i, err.Error())
			continue
		}

	}
}

func TestJSONReaderErrors(t *testing.T) {
	cases := []struct {
		text      string
		structure *dataset.Structure
		count     int
		err       string
	}{
		{"{\"a\":1}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 1, ""},
		{"{\"a\"\"b\":1}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 0, "Expected: ':' to separate key and value"},
		{"{:\"a\"1}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 0, "Expected: string"},
		{"{\"abc:def\"1}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 0, "Expected: ':' to separate key and value"},
		{"{\"a\"\x01:\x02\"b\"}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 0, "Expected: ':' to separate key and value"},
		{"{\"abc\",1,,,,,\"def\",2,,\"ghi\",3,,,\"jkl\"4:}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 0, "Expected: ':' to separate key and value"},
		{"{\"abc\":{\"inner\":1}}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 1, ""},
		{"{\"abc\":[1,2,3]}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 1, ""},
		{"{\"abc\":{\"inner\":[1,2,3]}}", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 1, ""},
		{"{\"abc\":1,", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 1, "Expected: string"},
		{"{\"abc\":1", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		}, 1, "Expected: separator ','"},
		{"[\"abc\",1]", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 2, ""},
		{"[]", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 0, ""},
		{"[{}]", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 1, ""},
		{"[\"abc\",1", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 2, "Expected: separator ','"},
		{"[\"abc\",1,", &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaArray,
		}, 3, "Expected: separator ','"},
	}

	for i, c := range cases {
		r, _ := NewJSONReader(c.structure, strings.NewReader(c.text))
		j := 0
		vs := []Entry{}
		for {
			ent, err := r.ReadEntry()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				if c.err == "" {
					t.Errorf("case %d error reading row %d: %s", i, j, err.Error())
				} else if c.err != err.Error() {
					t.Errorf("case %d error mismatch row %d: {%s} <> {%s}", i, j, c.err, err.Error())
				}
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
		{&dataset.Structure{Schema: map[string]interface{}{"type": "string"}}, []Entry{}, "[]", "invalid schema. root must be either an array or object type"},

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
		if w.tlt == "object" {
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
	w, err := NewJSONWriter(&dataset.Structure{Format: "json", Schema: dataset.BaseSchemaObject}, buf)
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
	w, err := NewJSONWriter(&dataset.Structure{Format: "json", Schema: dataset.BaseSchemaObject}, buf)
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
	st := &dataset.Structure{Format: "json", Schema: dataset.BaseSchemaObject}

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

func TestJSONPrettyWriter(t *testing.T) {
	st := &dataset.Structure{Schema: dataset.BaseSchemaArray}
	buf := &bytes.Buffer{}
	w, err := NewJSONPrettyWriter(st, buf, " ")
	if err != nil {
		t.Fatal(err)
	}

	err = w.WriteEntry(Entry{Value: map[string]string{"a": "hello"}})
	if err != nil {
		t.Fatal(err)
	}

	err = w.WriteEntry(Entry{Value: map[string]string{"b": "goodbye"}})
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	result := string(buf.Bytes())
	expect := `[
 {
  "a": "hello"
 },
 {
  "b": "goodbye"
 }
]`
	if result != expect {
		t.Errorf("result mismatch: expected: \"%s\", got: \"%s\"", result, expect)
	}
}

func BenchmarkJSONWriterObjects(b *testing.B) {
	const NumWrites = 1000
	st := &dataset.Structure{Format: "json", Schema: dataset.BaseSchemaObject}

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

func BenchmarkJSONReader(b *testing.B) {
	st := &dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray}

	for n := 0; n < b.N; n++ {
		file, err := os.Open(testdataFile("../dsio/testdata/movies/body.json"))
		if err != nil {
			b.Errorf("unexpected error: %s", err.Error())
		}
		r, err := NewJSONReader(st, file)
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
