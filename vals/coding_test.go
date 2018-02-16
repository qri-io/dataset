package vals

import (
	"bytes"
	"encoding/json"
	"testing"
)

var (
	array0  = &Array{String("a"), Boolean(false), Null(false), Integer(2), Number(23.5)}
	object0 = &Object{"city": String("toronto"), "pop": Integer(40000000), "avg_age": Number(55.5), "in_usa": Boolean(false)}
	array1  = &Array{*array0, *array0}
	array2  = &Array{*object0, *object0}
)

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

		// TODO - not passing:
		//   - expected: vals.Array{true, true, true},
		//   - got: vals.Array{false, false, false}
		// {`[null, null, null]`, Array{Null(true), Null(true), Null(true)}, ""},

		// TODO - not passing:
		//   - expected: vals.Array{vals.Array{"a", ...}, vals.Array{"a", ...}},
		//   - got: vals.Array{vals.Value(nil), vals.Value(nil)}
		// {`[["a", false, null, 2, 23.5],["a", false, null, 2, 23.5]]`, *array1, ""},

		// TODO - not passing:
		//   - expected: vals.Array{vals.Object{"city":"toronto"...}, vals.Object{"city":"toronto"...}},
		//   - got: vals.Array{vals.Value(nil), vals.Value(nil)}
		// {`[{ "city" : "toronto", "pop" : 40000000, "avg_age" : 55.5 , "in_usa" : false },{ "city" : "toronto", "pop" : 40000000, "avg_age" : 55.5 , "in_usa" : false }]`, *array2, ""},
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
