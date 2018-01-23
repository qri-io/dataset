package vals

import (
	"bytes"
	"encoding/json"
	"testing"
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
