package dsio

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/dataset/vals"
	"github.com/qri-io/jsonschema"
)

var (
	// {"l":{"url": "https://datatogether.org/activities/harvesting", "surtUrl": "(org,datatogether,)/activities/harvesting>", "timestamp": "2018-02-14T10:00:51.274376-05:00", "duration": 1284909634, "status": 200, "contentType": "text/html; charset=utf-8", "contentSniff": "text/html; charset=utf-8", "contentLength": 4192, "title": "\n\n\n     Harvesting Data Together \n\n", "hash": "1220b0c29592dc07147c7455a013bffa5dc3e183e1ab0607cd2cfeedf68ebe6f636f", "links": ["http://datatogether.org/activities/harvesting", "https://datatogether.org/css/style.css"]}}
	bigObj = `A1616CAB6375726C782E68747470733A2F2F64617461746F6765746865722E6F72672F616374697669746965732F68617276657374696E67677375727455726C782A286F72672C64617461746F6765746865722C292F616374697669746965732F68617276657374696E673E6974696D657374616D707820323031382D30322D31345431303A30303A35312E3237343337362D30353A3030686475726174696F6E1A4C962A426673746174757318C86B636F6E74656E74547970657818746578742F68746D6C3B20636861727365743D7574662D386C636F6E74656E74536E6966667818746578742F68746D6C3B20636861727365743D7574662D386D636F6E74656E744C656E677468191060657469746C6578230A0A0A202020202048617276657374696E67204461746120546F676574686572200A0A646861736878443132323062306332393539326463303731343763373435356130313362666661356463336531383365316162303630376364326366656564663638656265366636333666656C696E6B7382782D687474703A2F2F64617461746F6765746865722E6F72672F616374697669746965732F68617276657374696E67782668747470733A2F2F64617461746F6765746865722E6F72672F6373732F7374796C652E637373`
	bigVal = vals.NewObjectValue("l", &vals.Object{
		"url":           vals.String("https://datatogether.org/activities/harvesting"),
		"surtUrl":       vals.String("(org,datatogether,)/activities/harvesting>"),
		"timestamp":     vals.String("2018-02-14T10:00:51.274376-05:00"),
		"duration":      vals.Integer(1284909634),
		"status":        vals.Integer(200),
		"contentType":   vals.String("text/html; charset=utf-8"),
		"contentSniff":  vals.String("text/html; charset=utf-8"),
		"contentLength": vals.Integer(4192),
		"title":         vals.String("\n\n\n     Harvesting Data Together \n\n"),
		"hash":          vals.String("1220b0c29592dc07147c7455a013bffa5dc3e183e1ab0607cd2cfeedf68ebe6f636f"),
		"links": &vals.Array{
			vals.String("http://datatogether.org/activities/harvesting"),
			vals.String("https://datatogether.org/css/style.css"),
		},
	})
)

func TestCBORReaderOneValue(t *testing.T) {
	arrCases := []struct {
		data string
		val  vals.Value
		err  string
	}{
		{`5f`, nil, "invalid top level type"}, // indefinite string, not a valid dataset

		{`80`, nil, "EOF"},                          // []
		{`8000`, vals.Integer(0), ""},               // [0]
		{`8116`, vals.Integer(22), ""},              // [22]
		{`8117`, vals.Integer(23), ""},              // [23]
		{`811818`, vals.Integer(24), ""},            // [24]
		{`811901F4`, vals.Integer(500), ""},         // [500]
		{`811A004C4B40`, vals.Integer(5000000), ""}, // [5000000]
		{`8020`, vals.Integer(-1), ""},              // [-1]

		{`81FB4028AE147AE147AE`, vals.Number(12.34), ""},            // [12.34]
		{`81FB402A1D1F601797CC`, vals.Number(13.05688), ""},         // [13.05688]
		{`8163666F6F`, vals.String("foo"), ""},                      // ["foo"]
		{`81F5`, vals.Boolean(true), ""},                            // [true]
		{`81F4`, vals.Boolean(false), ""},                           // [false]
		{`81F6`, vals.Null(true), ""},                               // [null]
		{`81A0`, &vals.Object{}, ""},                                // [{}]
		{`81A163666F6FA0`, &vals.Object{"foo": &vals.Object{}}, ""}, // [{"foo":{}}]

		{`81782A286F72672C64617461746F6765746865722C292F616374697669746965732F68617276657374696E673E`, vals.String("(org,datatogether,)/activities/harvesting>"), ""}, // ["(org,datatogether,)/activities/harvesting>"]

		// TODO - currently don't support indefinite arrays.
		// {`9FFF`, obj, "EOF"},      // [] - indefinte array
		// {`9F00FF`, arr, ""},       // [0] - indefinite array
		// {`9F20FF`, arr, ""},       // [-1] - indefinite array
		// {`9F63666F6FFF`, arr, ""}, // ["foo"] - indefinite array
		// {`9FF4FF`, arr, ""},       // [false] - indefinite array
		// {`9FF5FF`, arr, ""},       // [true] - indefinite array
		// {`9FF6FF`, arr, ""},       // [null] - indefinite array

		// TODO - need to add tests for tag values
	}

	objCases := []struct {
		data string
		val  vals.Value
		err  string
	}{
		{`A0`, nil, "EOF"},                                                                        // {}
		{`A1616000`, vals.NewObjectValue("a", vals.Integer(0)), ""},                               // {"a":0}
		{`A1616217`, vals.NewObjectValue("b", vals.Integer(23)), ""},                              // {"b":23}
		{`A161631818`, vals.NewObjectValue("c", vals.Integer(24)), ""},                            // {"c":24}
		{`A161641901F4`, vals.NewObjectValue("d", vals.Integer(500)), ""},                         // {"d":500}
		{`A161651A004C4B40`, vals.NewObjectValue("e", vals.Integer(5000000)), ""},                 // {"e":5000000}
		{`A1616620`, vals.NewObjectValue("f", vals.Integer(-1)), ""},                              // {"f":-1}
		{`A16166FB4028AE147AE147AE`, vals.Number(12.34), ""},                                      // {"f":[12.34]}
		{`A1616763666F6F`, vals.NewObjectValue("g", vals.String("foo")), ""},                      // {"g":"foo"}
		{`A16168F5`, vals.NewObjectValue("h", vals.Boolean(true)), ""},                            // {"h":true}
		{`A16169F4`, vals.NewObjectValue("i", vals.Boolean(false)), ""},                           // {"i":false}
		{`A1616AF6`, vals.NewObjectValue("j", vals.Null(true)), ""},                               // {"j":null}
		{`A1616BA0`, vals.NewObjectValue("k", &vals.Object{}), ""},                                // {"k":{}}
		{`A1616CA163666F6FA0`, vals.NewObjectValue("l", &vals.Object{"foo": &vals.Object{}}), ""}, // {"l": {"foo":{}}}

		{`A1616C782A286F72672C64617461746F6765746865722C292F616374697669746965732F68617276657374696E673E`, vals.NewObjectValue("l", vals.String("(org,datatogether,)/activities/harvesting>")), ""}, // {"l":"(org,datatogether,)/activities/harvesting>"}
		{bigObj, bigVal, ""},

		// TODO - currently don't support indefinite maps
		// {`bfff`, vals.Object{}, "EOF"}, // {} - indefinte map

		// TODO - need to add tests for tag values
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

		v, err := rdr.ReadValue()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("array case %d error mistmatch. expected: %s got: %s", i, c.err, err)
			continue
		}

		if v != nil && !vals.Equal(c.val, v) {
			t.Errorf("array case %d value mismatch. expected: %#v got: %#v", i, c.val, v)
			continue
		}
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

		v, err := rdr.ReadValue()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("object case %d error mistmatch. expected: %s got: %s", i, c.err, err)
			continue
		}

		if v != nil && !vals.Equal(c.val, v) {
			t.Errorf("object case %d value mismatch. expected: %#v got: %#v", i, c.val, v)
			if v.Type() == vals.TypeInteger {
				t.Errorf("case %d int val: %d", i, v.Integer())
			}
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
		tc, err := dstest.NewTestCaseFromDir(fmt.Sprintf("testdata/cbor/%s", c.name), t)
		if err != nil {
			t.Errorf("case %d:%s error reading test case: %s", i, c.name, err.Error())
			continue
		}

		r, err := NewCBORReader(c.structure, tc.DataFile())
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
		vs := []vals.Value{}
		for {
			// TODO - inspect row output for well formed json
			v, err := r.ReadValue()
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
		entries   vals.Array
		out       string
		err       string
	}{
		{&dataset.Structure{}, vals.Array{}, "[]", "schema required for CBOR writer"},
		{&dataset.Structure{Schema: jsonschema.Must(`true`)}, vals.Array{}, "[]", "invalid schema. root must be either an array or object type"},

		{arrst, vals.Array{}, "80", ""},
		{objst, vals.Array{}, "a0", ""},

		{objst, vals.Array{vals.ObjectValue{"a", vals.String("hello")}, vals.ObjectValue{"b", vals.String("world")}}, `a261616568656c6c6f616265776f726c64`, ""},
		{arrst, vals.Array{vals.String("hello"), vals.String("world")}, `826568656c6c6f65776f726c64`, ""},
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
			if err := w.WriteValue(ent); err != nil {
				t.Errorf("case %d WriteValue error: %s", i, err.Error())
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

func TestCBORWriterNonObjectValue(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewCBORWriter(&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}, buf)
	if err != nil {
		t.Errorf("unexpected error creating writer: %s", err.Error())
		return
	}

	err = w.WriteValue(vals.Boolean(false))
	expect := `only vals.ObjectValue can be written to a JSON object writer`
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

	if err := w.WriteValue(vals.ObjectValue{"a", vals.String("foo")}); err != nil {
		t.Errorf("unexpected error writing key: %s", err.Error())
		return
	}

	err = w.WriteValue(vals.ObjectValue{"a", vals.Boolean(true)})
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

func TestCBORWriterCanonical(t *testing.T) {
	st := &dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaObject}
	vals := vals.Array{
		vals.ObjectValue{"a", vals.String("a")},
		vals.ObjectValue{"b", vals.String("b")},
		vals.ObjectValue{"c", vals.String("c")},
		vals.ObjectValue{"d", vals.String("d")},
		vals.ObjectValue{"e", vals.String("e")},
	}
	expect := `a56161616161626162616361636164616461656165`

	buf := &bytes.Buffer{}
	for i := 0; i < 150; i++ {
		w, err := NewCBORWriter(st, buf)
		if err != nil {
			t.Errorf("iteration %d error creating writer: %s", i, err.Error())
			return
		}
		for _, val := range vals {
			if err := w.WriteValue(val); err != nil {
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
