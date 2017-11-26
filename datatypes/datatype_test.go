package datatypes

import (
	"bytes"
	"errors"
	"github.com/qri-io/compare"
	"testing"
	"time"
)

func TestTypeString(t *testing.T) {
	cases := []struct {
		t      Type
		expect string
	}{
		{Unknown, ""},
		{Type(-1), ""},
		{Any, "any"},
		{String, "string"},
		{Integer, "integer"},
		{Float, "float"},
		{Boolean, "boolean"},
		{Date, "date"},
		{URL, "url"},
		{JSON, "json"},
	}

	for i, c := range cases {
		if c.t.String() != c.expect {
			t.Errorf("case %d mismatch. expected: %s. got: %s", i, c.expect, c.t.String())
			continue
		}
	}
}

func TestTypeFromString(t *testing.T) {
	cases := []struct {
		s      string
		expect Type
	}{
		{"", Unknown},
		{"foo", Unknown},
		{"any", Any},
		{"string", String},
		{"integer", Integer},
		{"float", Float},
		{"boolean", Boolean},
		{"date", Date},
		{"url", URL},
		{"json", JSON},
	}

	for i, c := range cases {
		if got := TypeFromString(c.s); got != c.expect {
			t.Errorf("case %d mismatch. expected: %s. got: %s", i, c.expect, got)
			continue
		}
	}
}

func TestTypeMarshalJSON(t *testing.T) {
	data, err := String.MarshalJSON()
	if err != nil {
		t.Errorf(err.Error())
	}
	if !bytes.Equal(data, []byte(`"string"`)) {
		t.Errorf("expected String.MarshalJSON to equal \"string\"")
	}
}

func TestTypeUnmarshalJSON(t *testing.T) {
	ty := Type(0)
	typ := &ty

	if err := typ.UnmarshalJSON([]byte(`"string"`)); err != nil {
		t.Error(err)
	}
	if *typ != String {
		t.Errorf("type mismatch. expected: String, got: %s", typ)
	}
}

func TestTypeParse(t *testing.T) {
	cases := []struct {
		typ    Type
		data   string
		parsed interface{}
		err    string
	}{
		// {Unknown, "", nil, ""},
		// {Unknown, "foo", nil, ""},
		// {Any, "any", nil, ""},
		{String, "hey", "hey", ""},
		{Integer, "1337", 1337, ""},
		{Float, "101.5", 101.5, ""},
		{Boolean, "false", false, ""},
		// {Date, "date", nil, ""},
		// {URL, "url", nil, ""},
		{JSON, "{\"data\":\"json\"}", map[string]interface{}{"data": "json"}, ""},
	}

	for i, c := range cases {
		got, err := c.typ.Parse([]byte(c.data))
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mistmatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if compare.Interface(c.parsed, got); err != nil {
			t.Errorf("case %d error mistmatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
	}
}

func TestParseDatatype(t *testing.T) {
	cases := []struct {
		value  string
		expect Type
	}{
		{"{}", JSON},
		{"[]", JSON},
		{"1", Integer},
		{"1.5", Float},
		{"false", Boolean},
		{"true", Boolean},
		{"2015-09-03T13:27:52Z", Date},
		{"", String},
	}
	for i, c := range cases {
		got := ParseDatatype([]byte(c.value))
		if c.expect != got {
			t.Errorf("case %d response mismatch. expected: %s, got: %s", i, c.expect, got)
			continue
		}
	}
}

// TODO
func TestParseAny(t *testing.T) {
	cases := []struct {
		input  []byte
		expect interface{}
		err    error
	}{}
	for i, c := range cases {
		value, got := ParseAny(c.input)
		if value != c.expect {
			t.Errorf("case %d value mismatch. expected: %s, got: %s", i, c.expect, value)
		}
		if c.err != got {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
		}
	}
}

func TestParseString(t *testing.T) {
	cases := []struct {
		input  []byte
		expect string
		err    error
	}{
		{[]byte("foo"), "foo", nil},
	}
	for i, c := range cases {
		value, got := ParseString(c.input)
		if value != c.expect {
			t.Errorf("case %d value mismatch. expected: %s, got: %s", i, c.expect, value)
		}
		if c.err != got {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
		}
	}
}

func TestParseFloat(t *testing.T) {
	cases := []struct {
		input  []byte
		expect float64
		err    error
	}{
		{[]byte("1234567890"), float64(1234567890), nil},
		{[]byte("12345.67890"), float64(12345.67890), nil},
		{[]byte("-12345.67890"), float64(-12345.67890), nil},
	}
	for i, c := range cases {
		value, got := ParseFloat(c.input)
		if value != c.expect {
			t.Errorf("case %d value mismatch. expected: %s, got: %s", i, c.expect, value)
		}
		if c.err != got {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
		}
	}
}

func TestParseInteger(t *testing.T) {
	cases := []struct {
		input  []byte
		expect int64
		err    error
	}{
		{[]byte("1234567890"), int64(1234567890), nil},
		{[]byte("12345.67890"), 0, errors.New(`strconv.ParseInt: parsing "12345.67890": invalid syntax`)},
		{[]byte("-12345.67890"), 0, errors.New(`strconv.ParseInt: parsing "-12345.67890": invalid syntax`)},
	}
	for i, c := range cases {
		value, got := ParseInteger(c.input)
		if value != c.expect {
			t.Errorf("case %d value mismatch. expected: %s, got: %s", i, c.expect, value)
		}
		if got != nil {
			if c.err != nil && got.Error() != c.err.Error() {
				t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
			}
		}
	}
}

func TestParseBoolean(t *testing.T) {
	cases := []struct {
		input  []byte
		expect bool
		err    error
	}{}
	for i, c := range cases {
		value, got := ParseBoolean(c.input)
		if value != c.expect {
			t.Errorf("case %d value mismatch. expected: %s, got: %s", i, c.expect, value)
		}
		if c.err != got {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
		}
	}
}

func TestJSONArrayOrObject(t *testing.T) {
	cases := []struct {
		data, expect, err string
	}{
		{"", "", "invalid json data"},
		{"[", "array", ""},
		{"[{", "array", ""},
		{"{", "object", ""},
		{"{[", "object", ""},
	}
	for i, c := range cases {
		got, err := JSONArrayOrObject([]byte(c.data))
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err.Error())
			continue
		}
		if got != c.expect {
			t.Errorf("case %d result mismatch. expected: %s, got: %s", i, c.expect, got)
			continue
		}
	}
}

func TestParseJSON(t *testing.T) {
	cases := []struct {
		input  []byte
		expect interface{}
		err    string
	}{
		{[]byte{}, nil, "invalid json data"},
		{[]byte("string"), nil, "invalid json data"},
		{[]byte("5.0"), nil, "invalid json data"},
		{[]byte("false"), nil, "invalid json data"},
		{[]byte("foo,bar"), nil, "invalid json data"},
		{[]byte(`{ "a" : "b" }`), map[string]interface{}{"a": "b"}, ""},
		{[]byte(`[{ "a" : "b" }]`), []interface{}{map[string]interface{}{"a": "b"}}, ""},
	}
	for i, c := range cases {
		value, err := ParseJSON(c.input)

		if err := compare.Interface(c.expect, value); err != nil {
			t.Errorf("case %d value mismatch. expected: %s, got: %s, error %s", i, c.expect, value, err.Error())
			continue
		}
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
	}
}

// TODO
func TestParseDate(t *testing.T) {
	cases := []struct {
		input  []byte
		expect *time.Time
		err    error
	}{}
	for i, c := range cases {
		value, got := ParseDate(c.input)
		if value.String() != c.expect.String() {
			t.Errorf("case %d value mismatch. expected: %s, got: %s", i, c.expect, value)
		}
		if c.err != got {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
		}
	}
}

func TestParseURL(t *testing.T) {
	cases := []struct {
		input  string
		expect string
		err    error
	}{
		{"apple.com", "apple.com", nil},
		{"http://qri.io", "http://qri.io", nil},
		{"https://beastmo.de", "https://beastmo.de", nil},
		{"https://beastmo.de/this/path", "https://beastmo.de/this/path", nil},
		{"https://beastmo.de/this/path?input=blah", "https://beastmo.de/this/path?input=blah", nil},
		{"https://beastmo.de/this/path?input=blah#fragment", "https://beastmo.de/this/path?input=blah#fragment", nil},
		{"https://beastmo.de/this/path?input=blah#bad fragment", "https://beastmo.de/this/path?input=blah#bad%20fragment", nil},
	}
	for i, c := range cases {
		value, got := ParseURL([]byte(c.input))
		if value.String() != c.expect {
			t.Errorf("case %d value mismatch. expected: %s, got: %s", i, c.expect, value.String())
		}
		if got != nil {
			if c.err != nil && got.Error() != c.err.Error() {
				t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
			}
		}
	}
}

func TestValueToString(t *testing.T) {
	cases := []struct {
		t      Type
		v      interface{}
		expect string
		err    string
	}{
		{Unknown, "", "", "cannot get string value of unknown datatype"},
		{Integer, 234, "234", ""},
		{Integer, "234", "", "234 is not an integer value"},
		{Float, float32(234.0), "234", ""},
		{Float, float32(234.12339782714844), "234.12339782714844", ""},
		{Float, "234", "", "234 is not a float value"},
		{Boolean, false, "false", ""},
		{Boolean, true, "true", ""},
		{Boolean, "234", "", "234 is not a boolean value"},
		{JSON, map[string]interface{}{"a": "b"}, `{"a":"b"}`, ""},
		// {JSON, "234", "", "234 is not a json value"},
		{Date, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC), "2001-01-01T00:00:00Z", ""},
		{Date, "234", "", "234 is not a date value"},
		{String, "foo", "foo", ""},
		{String, 234, "", "234 is not a string value"},
	}

	for i, c := range cases {
		got, err := c.t.ValueToString(c.v)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if c.expect != got {
			t.Errorf("case %d mismatch. expected: '%s', got: '%s'", i, c.expect, got)
			continue
		}
	}
}

func TestValueToBytes(t *testing.T) {
	cases := []struct {
		t      Type
		v      interface{}
		expect string
		err    string
	}{
		{Unknown, "", "", "cannot get string value of unknown datatype"},
		{Integer, 234, "234", ""},
		{Integer, "234", "", "234 is not an integer value"},
		{Float, float32(234.0), "234", ""},
		{Float, float32(234.12339782714844), "234.12339782714844", ""},
		{Float, "234", "", "234 is not a float value"},
		{Boolean, false, "false", ""},
		{Boolean, true, "true", ""},
		{Boolean, "234", "", "234 is not a boolean value"},
		{JSON, map[string]interface{}{"a": "b"}, `{"a":"b"}`, ""},
		// {JSON, "234", "", "234 is not a json value"},
		{Date, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC), "2001-01-01T00:00:00Z", ""},
		{Date, "234", "", "234 is not a date value"},
		{String, "foo", "foo", ""},
		{String, 234, "", "234 is not a string value"},
	}

	for i, c := range cases {
		got, err := c.t.ValueToBytes(c.v)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if !bytes.Equal([]byte(c.expect), got) {
			t.Errorf("case %d mismatch. expected: '%s', got: '%s'", i, c.expect, string(got))
			continue
		}
	}
}
