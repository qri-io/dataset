package datatypes

import (
	"bytes"
	"errors"
	"github.com/qri-io/compare"
	"math"
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
	cases := []struct {
		ty     Type
		s      string
		expect []byte
		err    error
	}{
		{Unknown, "Unknown", []byte(`""`), nil},
		{Type(-1), "Type(-1)", []byte(`""`), nil},
		{Any, "Any", []byte(`"any"`), nil},
		{String, "String", []byte(`"string"`), nil},
		{Integer, "Integer", []byte(`"integer"`), nil},
		{Float, "Float", []byte(`"float"`), nil},
		{Boolean, "Boolean", []byte(`"boolean"`), nil},
		{Date, "Data", []byte(`"date"`), nil},
		{URL, "URL", []byte(`"url"`), nil},
		{JSON, "JSON", []byte(`"json"`), nil},
	}
	for i, c := range cases {
		data, err := c.ty.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mistmatch. expected: %s, got: %s", i, c.err, err)
		}
		if !bytes.Equal(data, c.expect) {
			t.Errorf("expected %s.MarshalJSON to equal %s, got %s", c.s, c.expect, data)
		}
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
		// {"https://golang.org", URL},
		{"Go to https://golang.org for more information", String},
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
		{[]byte("1.797693134862315708145274237317043567981e+308"), math.MaxFloat64, nil},
		{[]byte("2e+308"), math.Inf(0), errors.New(`strconv.ParseFloat: parsing "2e+308": value out of range`)},
		{[]byte("4.940656458412465441765687928682213723651e-324"), math.SmallestNonzeroFloat64, nil},
		{[]byte("1.940e-324"), float64(0), nil},
	}
	for i, c := range cases {
		value, got := ParseFloat(c.input)
		if value != c.expect {
			t.Errorf("case %d value mismatch. expected: %e, got: %e", i, c.expect, value)
		}
		if got != nil {
			if c.err != nil && got.Error() != c.err.Error() {
				t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, got)
			}
		}
	}
}

func TestParseInteger(t *testing.T) {
	cases := []struct {
		input  []byte
		expect int64
		err    error
	}{
		{[]byte(""), 0, errors.New(`strconv.ParseInt: parsing "": invalid syntax`)},
		{[]byte("9223372036854775807"), math.MaxInt64, nil},
		{[]byte("9223372036854775808"), math.MaxInt64, errors.New(`strconv.ParseInt: parsing "9223372036854775808": value out of range`)},
		{[]byte("-9223372036854775808"), math.MinInt64, nil},
		{[]byte("-9223372036854775809"), math.MinInt64, errors.New(`strconv.ParseInt: parsing "-9223372036854775809": value out of range`)},
		{[]byte("1234567890"), int64(1234567890), nil},
		{[]byte("12345.67890"), 0, errors.New(`strconv.ParseInt: parsing "12345.67890": invalid syntax`)},
		{[]byte("-12345.67890"), 0, errors.New(`strconv.ParseInt: parsing "-12345.67890": invalid syntax`)},
	}
	for i, c := range cases {
		value, got := ParseInteger(c.input)
		if value != c.expect {
			t.Errorf("case %d value mismatch. expected: %d, got: %d", i, c.expect, value)
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

func TestIsInteger(t *testing.T) {
	cases := []struct {
		b      []byte
		expect bool
	}{
		{[]byte(""), false},
		{[]byte("1"), true},
		{[]byte("367890"), true},
		{[]byte("1.2"), false},
		{[]byte("foo"), false},
		{[]byte("9223372036854775808"), true},
		{[]byte("890oasdfg dfgh89"), false},
		{[]byte("[123]"), false},
	}
	for _, c := range cases {
		got := IsInteger(c.b)
		if got != c.expect {
			t.Errorf("case IsInteger: %s - expected: '%t', got: '%t'", c.b, c.expect, got)
		}
	}
}

var result bool
var resultType Type
var resultInterface interface{}

func benchmarkParseDatatype(value []byte, b *testing.B) {
	var t Type
	for n := 0; n < b.N; n++ {
		t = ParseDatatype(value)
	}
	resultType = t
}

// best case would be an empty slice of bytes
func BenchmarkParseDatatypeBestCase(b *testing.B) {
	benchmarkParseDatatype([]byte(""), b)
}

// worst case is getting datatype from JSON, because in order to validate that it is JSON, you have to parse the entire slice of bytes
func BenchmarkParseDatatypeWorstCase(b *testing.B) {
	json := "{'id': '0001','type': 'donut','name': 'Cake','ppu': 0.55,'batters':{'batter':[{ 'id': '1001', 'type': 'Regular' },{ 'id': '1002', 'type': 'Chocolate' },{ 'id': '1003', 'type': 'Blueberry' },{ 'id': '1004', 'type': 'Devil's Food' }]},'topping':[{ 'id': '5001', 'type': 'None' },{ 'id': '5002', 'type': 'Glazed' },{ 'id': '5005', 'type': 'Sugar' },{ 'id': '5007', 'type': 'Powdered Sugar' },{ 'id': '5006', 'type': 'Chocolate with Sprinkles' },{ 'id': '5003', 'type': 'Chocolate' },{ 'id': '5004', 'type': 'Maple' }]}"
	benchmarkParseDatatype([]byte(json), b)
}

func benchmarkParse(value []byte, t Type, b *testing.B) {
	var i interface{}
	for n := 0; n < b.N; n++ {
		i, _ = t.Parse(value)
	}
	resultInterface = i
}

func BenchmarkParseBestCase(b *testing.B) {
	var t Type = String
	benchmarkParse([]byte(""), t, b)
}

func BenchmarkParseWorstCase(b *testing.B) {
	var t Type = JSON
	json := "{'id': '0001','type': 'donut','name': 'Cake','ppu': 0.55,'batters':{'batter':[{ 'id': '1001', 'type': 'Regular' },{ 'id': '1002', 'type': 'Chocolate' },{ 'id': '1003', 'type': 'Blueberry' },{ 'id': '1004', 'type': 'Devil's Food' }]},'topping':[{ 'id': '5001', 'type': 'None' },{ 'id': '5002', 'type': 'Glazed' },{ 'id': '5005', 'type': 'Sugar' },{ 'id': '5007', 'type': 'Powdered Sugar' },{ 'id': '5006', 'type': 'Chocolate with Sprinkles' },{ 'id': '5003', 'type': 'Chocolate' },{ 'id': '5004', 'type': 'Maple' }]}"
	benchmarkParse([]byte(json), t, b)
}

func benchmarkIsInteger(x []byte, b *testing.B) {
	var r bool
	for n := 0; n < b.N; n++ {
		r = IsInteger(x)
	}
	result = r
}

// IsInteger best case: empty slice of bytes
func BenchmarkIsIntegerBestCase(b *testing.B) {
	var x []byte
	benchmarkIsInteger(x, b)
}

// IsInteger worse case: float
func BenchmarkIsIntegerWorstCase(b *testing.B) {
	benchmarkIsInteger([]byte("12.34"), b)
}
