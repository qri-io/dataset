package vals

import (
	"bytes"
	"encoding/json"
	"testing"
)

var (
	array0  = &Array{String("a"), Boolean(false), Null(true), Integer(2), Number(23.5)}
	object0 = &Object{"city": String("toronto"), "pop": Integer(40000000), "avg_age": Number(55.5), "in_usa": Boolean(false)}
	array1  = &Array{*array0, *array0}
	array2  = &Array{*object0, *object0}
)

func TestConvertDecoded(t *testing.T) {
	cases := []struct {
		in     interface{}
		expect Value
		err    string
	}{
		{map[string]interface{}{}, &Object{}, ""},
		{map[string]interface{}{
			"a": 0,
			"b": float64(0),
			"c": nil,
			"d": true,
			"e": "foo",
			"f": []interface{}{},
			"g": map[string]interface{}{},
			"h": uint8(0),
			"i": uint16(0),
			"j": uint64(0),
			"k": int32(0),
			"l": int64(0),
			"m": map[interface{}]interface{}{},
		}, &Object{
			"a": Integer(0),
			"b": Number(0),
			"c": Null(true),
			"d": Boolean(true),
			"e": String("foo"),
			"f": &Array{},
			"g": &Object{},
			"h": Integer(0),
			"i": Integer(0),
			"j": Integer(0),
			"k": Integer(0),
			"l": Integer(0),
			"m": &Object{},
		}, ""},
	}

	for i, c := range cases {
		got, err := ConvertDecoded(c.in)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if !Equal(c.expect, got) {
			t.Errorf("case %d result mismatch. epxected: %#v, got: %#v", i, c.expect, got)
			continue
		}
	}
}

func TestUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		expect Value
		err    string
	}{
		{`"foo"`, String("foo"), ""},
		{`123`, Integer(123), ""},
		{`123.45`, Number(123.45), ""},
		{`{ "city" : "toronto", "pop" : 40000000, "avg_age" : 55.5 , "in_usa" : false }`, *object0, ""},
		{`["a", false, null, 2, 23.5]`, *array0, ""},
		{`[null, null, null]`, Array{Null(true), Null(true), Null(true)}, ""},
		{`[["a", false, null, 2, 23.5],["a", false, null, 2, 23.5]]`, *array1, ""},
		{`[{ "city" : "toronto", "pop" : 40000000, "avg_age" : 55.5 , "in_usa" : false },{ "city" : "toronto", "pop" : 40000000, "avg_age" : 55.5 , "in_usa" : false }]`, *array2, ""},
	}
	for i, c := range cases {
		got, err := UnmarshalJSON([]byte(c.input))
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if !Equal(c.expect, got) {
			t.Errorf("case %d result mismatch. expected: %#v, got: %#v", i, c.expect, got)
			continue
		}
	}
}

func TestMarshalJSON(t *testing.T) {
	d := Array{
		Object{"foo": Boolean(false)},
		Boolean(true),
		Integer(12),
		Null(true),
		Number(123.456),
		Array{String("foo"), String("bar")},
	}

	b, err := json.Marshal(d)
	if err != nil {
		t.Errorf("unexpected error marshaling to JSON: %s", err.Error())
		return
	}

	expect := `[{"foo":false},true,12,null,123.456,["foo","bar"]]`
	if !bytes.Equal([]byte(expect), b) {
		t.Errorf("byte mismatch. expected: %s, got: %s", expect, string(b))
	}
}
