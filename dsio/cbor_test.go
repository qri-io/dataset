package dsio

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/jsonschema"
)

var (
	// {"l":{"url": "https://datatogether.org/activities/harvesting", "surtUrl": "(org,datatogether,)/activities/harvesting>", "timestamp": "2018-02-14T10:00:51.274376-05:00", "duration": 1284909634, "status": 200, "contentType": "text/html; charset=utf-8", "contentSniff": "text/html; charset=utf-8", "contentLength": 4192, "title": "\n\n\n     Harvesting Data Together \n\n", "hash": "1220b0c29592dc07147c7455a013bffa5dc3e183e1ab0607cd2cfeedf68ebe6f636f", "links": ["http://datatogether.org/activities/harvesting", "https://datatogether.org/css/style.css"]}}
	bigObj = `A1616CAB6375726C782E68747470733A2F2F64617461746F6765746865722E6F72672F616374697669746965732F68617276657374696E67677375727455726C782A286F72672C64617461746F6765746865722C292F616374697669746965732F68617276657374696E673E6974696D657374616D707820323031382D30322D31345431303A30303A35312E3237343337362D30353A3030686475726174696F6E1A4C962A426673746174757318C86B636F6E74656E74547970657818746578742F68746D6C3B20636861727365743D7574662D386C636F6E74656E74536E6966667818746578742F68746D6C3B20636861727365743D7574662D386D636F6E74656E744C656E677468191060657469746C6578230A0A0A202020202048617276657374696E67204461746120546F676574686572200A0A646861736878443132323062306332393539326463303731343763373435356130313362666661356463336531383365316162303630376364326366656564663638656265366636333666656C696E6B7382782D687474703A2F2F64617461746F6765746865722E6F72672F616374697669746965732F68617276657374696E67782668747470733A2F2F64617461746F6765746865722E6F72672F6373732F7374796C652E637373`
	bigVal = Entry{Key: "l", Value: map[string]interface{}{
		"url":           "https://datatogether.org/activities/harvesting",
		"surtUrl":       "(org,datatogether,)/activities/harvesting>",
		"timestamp":     "2018-02-14T10:00:51.274376-05:00",
		"duration":      int64(1284909634),
		"status":        int64(200),
		"contentType":   "text/html; charset=utf-8",
		"contentSniff":  "text/html; charset=utf-8",
		"contentLength": int64(4192),
		"title":         "\n\n\n     Harvesting Data Together \n\n",
		"hash":          "1220b0c29592dc07147c7455a013bffa5dc3e183e1ab0607cd2cfeedf68ebe6f636f",
		"links": []interface{}{
			"http://datatogether.org/activities/harvesting",
			"https://datatogether.org/css/style.css",
		},
	}}
)

// TODO(dustmop): Tag support.
// TODO(dustmop): Test illegal chunks.
// TODO(dustmop): Move indefinite streams to their own test, test that 0xff correctly returns EOF.

func TestCBORReaderOneArrayEntry(t *testing.T) {
	arrCases := []struct {
		data string
		val  interface{}
		err  string
	}{
		{`5f`, nil, "invalid top level type"}, // indefinite string, not a valid dataset

		{`80`, nil, "EOF"},                   // []
		{`8000`, int64(0), ""},               // [0]
		{`8116`, int64(22), ""},              // [22]
		{`8117`, int64(23), ""},              // [23]
		{`811818`, int64(24), ""},            // [24]
		{`811901F4`, int64(500), ""},         // [500]
		{`811A004C4B40`, int64(5000000), ""}, // [5000000]
		{`8020`, int64(-1), ""},              // [-1]

		{`81FB4028AE147AE147AE`, 12.34, ""},    // [12.34]
		{`81FB402A1D1F601797CC`, 13.05688, ""}, // [13.05688]
		{`8163666F6F`, "foo", ""},              // ["foo"]
		{`81F5`, true, ""},                     // [true]
		{`81F4`, false, ""},                    // [false]
		{`81F6`, nil, ""},                      // [null]
		{`81A0`, map[string]interface{}{}, ""}, // [{}]

		// array - [[1,2,3]]
		{`8183010203`, []interface{}{int64(1), int64(2), int64(3)}, ""},
		// map   - [{"a":1,"b":2}]
		{`81A2616101616202`, map[string]interface{}{"a": int64(1), "b": int64(2)}, ""},
		// bytes - [[0x01,0x02,0x03]]
		{`8143010203`, []byte{0x01, 0x02, 0x03}, ""},

		// array of indeterminate length
		{`819f010203ff`, []interface{}{int64(1), int64(2), int64(3)}, ""},
		// map of indeterminate length
		{`81bf616101616202ff`, map[string]interface{}{"a": int64(1), "b": int64(2)}, ""},
		// bytes of indeterminate length
		{`815f43010203ff`, []byte{1, 2, 3}, ""},
		// string of indeterminate length
		{`817f63636174ff`, "cat", ""},
		// bytes of chunks
		{`815f4201024103ff`, []byte{1, 2, 3}, ""},
		// string of chunks
		{`817f6263616174ff`, "cat", ""},

		{`81782A286F72672C64617461746F6765746865722C292F616374697669746965732F68617276657374696E673E`, "(org,datatogether,)/activities/harvesting>", ""}, // ["(org,datatogether,)/activities/harvesting>"]

		// Top-level array of indetermine size
		{`9f16ff`, int64(22), ""}, // [22]
	}

	for i, c := range arrCases {
		d, err := hex.DecodeString(c.data)
		if err != nil {
			t.Errorf("array case %d error decoding hex string: %s", i, err.Error())
		}
		rdr, err := NewCBORReader(&dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaArray}, bytes.NewReader(d))
		if err != nil {
			t.Errorf("array case %d error creating reader: %s", i, err.Error())
			continue
		}

		v, err := rdr.ReadEntry()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("array case %d error mistmatch. expected: %s got: %s", i, c.err, err)
			continue
		}

		if !reflect.DeepEqual(c.val, v.Value) {
			t.Errorf("array case %d value mismatch. expected: type %T, value %#v got: type %T, value %#v", i, c.val, c.val, v.Value, v.Value)
			continue
		}
	}
}

func TestCBORReaderOneObjectEntry(t *testing.T) {

	objCases := []struct {
		data string
		val  Entry
		err  string
	}{
		{`A0`, Entry{}, "EOF"},                                             // {}
		{`A1616100`, Entry{Key: "a", Value: int64(0)}, ""},                 // {"a":0}
		{`A1616217`, Entry{Key: "b", Value: int64(23)}, ""},                // {"b":23}
		{`A161631818`, Entry{Key: "c", Value: int64(24)}, ""},              // {"c":24}
		{`A161641901F4`, Entry{Key: "d", Value: int64(500)}, ""},           // {"d":500}
		{`A161651A004C4B40`, Entry{Key: "e", Value: int64(5000000)}, ""},   // {"e":5000000}
		{`A1616620`, Entry{Key: "f", Value: int64(-1)}, ""},                // {"f":-1}
		{`A16166FB4028AE147AE147AE`, Entry{Key: "f", Value: 12.34}, ""},    // {"f":[12.34]}
		{`A1616763666F6F`, Entry{Key: "g", Value: "foo"}, ""},              // {"g":"foo"}
		{`A16168F5`, Entry{Key: "h", Value: true}, ""},                     // {"h":true}
		{`A16169F4`, Entry{Key: "i", Value: false}, ""},                    // {"i":false}
		{`A1616AF6`, Entry{Key: "j", Value: nil}, ""},                      // {"j":null}
		{`A1616BA0`, Entry{Key: "k", Value: map[string]interface{}{}}, ""}, // {"k":{}}

		{`A1616CA163666F6FA0`, Entry{Key: "l", Value: map[string]interface{}{"foo": map[string]interface{}{}}}, ""},                                                                  // {"l": {"foo":{}}}
		{`A1616C782A286F72672C64617461746F6765746865722C292F616374697669746965732F68617276657374696E673E`, Entry{Key: "l", Value: "(org,datatogether,)/activities/harvesting>"}, ""}, // {"l":"(org,datatogether,)/activities/harvesting>"}
		{bigObj, bigVal, ""},

		// Top-level map of indetermine size
		{`bf616100ff`, Entry{Key: "a", Value: int64(0)}, ""}, // {"a":0}
	}

	for i, c := range objCases {
		d, err := hex.DecodeString(c.data)
		if err != nil {
			t.Errorf("object case %d error decoding hex string: %s", i, err.Error())
		}
		rdr, err := NewCBORReader(&dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaObject}, bytes.NewReader(d))
		if err != nil {
			t.Errorf("object case %d error creating reader: %s", i, err.Error())
			continue
		}

		v, err := rdr.ReadEntry()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("object case %d error mistmatch. expected: %s got: %s", i, c.err, err)
			continue
		}

		if v.Key != c.val.Key {
			t.Errorf("object case %d key mismatch. expected: %s got: %s", i, c.val.Key, v.Key)
			continue
		}

		if !reflect.DeepEqual(c.val.Value, v.Value) {
			t.Errorf("object case %d value mismatch. expected: %#v got: %#v", i, c.val, v)
			continue
		}
	}
}

func TestCBORReaderFile(t *testing.T) {
	cases := []struct {
		name      string
		structure *dataset.Structure
		count     int
		err       string
	}{
		{"city", &dataset.Structure{}, 0, "schema required for CBOR reader"},
		{"city", &dataset.Structure{Schema: jsonschema.Must(`false`)}, 0, "invalid schema. root must be either an array or object type"},
		{"city", &dataset.Structure{
			Format: dataset.CBORDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 6, ""},

		// {"sitemap_object", &dataset.Structure{
		// 	Format: dataset.CBORDataFormat,
		// 	Schema: dataset.BaseSchemaObject,
		// }, 7, ""},

		{"links_object", &dataset.Structure{
			Format: dataset.CBORDataFormat,
			Schema: dataset.BaseSchemaObject,
		}, 20, ""},
		{"links_array", &dataset.Structure{
			Format: dataset.CBORDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 20, ""},
		{"array", &dataset.Structure{
			Format: dataset.CBORDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 10, ""},

		{"object", &dataset.Structure{
			Format: dataset.CBORDataFormat,
			Schema: dataset.BaseSchemaObject,
		}, 10, ""},
		// {"craigslist", &dataset.Structure{
		// 	Format: dataset.CBORDataFormat,
		// 	Schema: dataset.BaseSchemaArray,
		// }, 1200, ""},
		{"sitemap", &dataset.Structure{
			Format: dataset.CBORDataFormat,
			Schema: dataset.BaseSchemaObject,
		}, 1, ""},

		{"flourinated_compounds_in_fast_food_packaging", &dataset.Structure{
			Format: dataset.CBORDataFormat,
			Schema: dataset.BaseSchemaArray,
		}, 25, ""},
	}

	for i, c := range cases {
		tc, err := dstest.NewTestCaseFromDir(fmt.Sprintf("testdata/cbor/%s", c.name))
		if err != nil {
			t.Errorf("case %d:%s error reading test case: %s", i, c.name, err.Error())
			continue
		}

		r, err := NewCBORReader(c.structure, tc.BodyFile())
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
			v, err := r.ReadEntry()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				t.Errorf("case %d %s error reading row %d: %s", i, c.name, j, err.Error())
				break
			}
			vs = append(vs, v)
			j++
		}

		if c.count != j {
			t.Errorf("case %d %s count mismatch. expected: %d, got: %d", i, c.name, c.count, j)
			t.Log(vs)
			continue
		}
	}
}

func TestCBORWriter(t *testing.T) {
	objst := &dataset.Structure{Schema: dataset.BaseSchemaObject}
	arrst := &dataset.Structure{Schema: dataset.BaseSchemaArray}

	cases := []struct {
		structure *dataset.Structure
		entries   []Entry
		out       string
		err       string
	}{
		{&dataset.Structure{}, []Entry{}, "[]", "schema required for CBOR writer"},
		{&dataset.Structure{Schema: jsonschema.Must(`true`)}, []Entry{}, "[]", "invalid schema. root must be either an array or object type"},

		{arrst, []Entry{}, "80", ""},
		{objst, []Entry{}, "a0", ""},

		{objst, []Entry{{Key: "a", Value: "hello"}, {Key: "b", Value: "world"}}, `a261616568656c6c6f616265776f726c64`, ""},
		{arrst, []Entry{{Value: "hello"}, {Value: "world"}}, `826568656c6c6f65776f726c64`, ""},
	}

	for i, c := range cases {
		buf := &bytes.Buffer{}
		w, err := NewCBORWriter(c.structure, buf)
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

		str := hex.EncodeToString(buf.Bytes())
		if str != c.out {
			t.Errorf("case %d result mismatch. expected:\n%s\ngot:\n%s", i, c.out, str)
		}
	}
}

func TestCBORWriterNonObjectEntry(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewCBORWriter(&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}, buf)
	if err != nil {
		t.Errorf("unexpected error creating writer: %s", err.Error())
		return
	}

	err = w.WriteEntry(Entry{Value: false})
	expect := `Key cannot be empty`
	if err.Error() != expect {
		t.Errorf("error mismatch. expected: %s. got: %s", expect, err.Error())
		return
	}
}

func TestCBORWriterDoubleKey(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewCBORWriter(&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}, buf)
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

	expect := `key already written: 'a'`
	if err.Error() != expect {
		t.Errorf("error mismatch. expected: %s. got: %s", expect, err.Error())
		return
	}
}

func TestCBORWriterCanonical(t *testing.T) {
	st := &dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaObject}
	vals := []Entry{
		{Key: "a", Value: "a"},
		{Key: "b", Value: "b"},
		{Key: "c", Value: "c"},
		{Key: "d", Value: "d"},
		{Key: "e", Value: "e"},
	}
	expect := `a56161616161626162616361636164616461656165`

	buf := &bytes.Buffer{}
	for i := 0; i < 150; i++ {
		w, err := NewCBORWriter(st, buf)
		if err != nil {
			t.Errorf("iteration %d error creating writer: %s", i, err.Error())
			return
		}
		for _, ent := range vals {
			if err := w.WriteEntry(ent); err != nil {
				t.Errorf("iteration %d error writing value: %s", i, err.Error())
				return
			}
		}

		if err := w.Close(); err != nil {
			t.Errorf("iteration %d error closing writer: %s", i, err.Error())
			return
		}

		str := hex.EncodeToString(buf.Bytes())
		if str != expect {
			t.Errorf("iteration %d produced non-canonical result. expected: %s, got: %s", i, expect, str)
			return
		}

		buf.Reset()
	}
}

func BenchmarkCBORWriterArrays(b *testing.B) {
	const NumWrites = 1000
	st := &dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaObject}

	for n := 0; n < b.N; n++ {
		buf := &bytes.Buffer{}
		w, err := NewCBORWriter(st, buf)
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

func BenchmarkCBORWriterObjects(b *testing.B) {
	const NumWrites = 1000
	st := &dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaObject}

	for n := 0; n < b.N; n++ {
		buf := &bytes.Buffer{}
		w, err := NewCBORWriter(st, buf)
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

func BenchmarkCBORReader(b *testing.B) {
	st := &dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaArray}

	for n := 0; n < b.N; n++ {
		file, err := os.Open(testdataFile("../dsio/testdata/movies/body.cbor"))
		if err != nil {
			b.Errorf("unexpected error: %s", err.Error())
		}
		r, err := NewCBORReader(st, file)
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
