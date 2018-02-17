package vals

import (
	"testing"
)

func TestEqual(t *testing.T) {
	cases := []struct {
		a, b   Value
		expect bool
	}{
		{Array{Number(1)}, Array{Number(1)}, true},
		{Array{Number(1)}, Array{Number(2)}, false},
		{Object{"a": String("a")}, Object{"a": String("a")}, true},
		{Object{"a": String("a")}, Object{"a": String("b")}, false},
		{String("a"), String("a"), true},
		{String("a"), String("b"), false},
		{Boolean(true), Boolean(true), true},
		{Boolean(true), Boolean(false), false},
		{Integer(1), Integer(1), true},
		{Integer(1), Integer(2), false},
		{Number(1.1), Number(1.1), true},
		{Number(1.1), Number(1.11), false},
	}

	for i, c := range cases {
		got := Equal(c.a, c.b)
		if got != c.expect {
			t.Errorf("case: %d. %v == %v != %t", i, c.a, c.b, c.expect)
		}
	}
}

func TestCompareTypeBytes(t *testing.T) {
	cases := []struct {
		a, b   string
		t      Type
		expect int
		err    string
	}{
		{"0", "0", TypeUnknown, 0, "invalid type comparison"},
		{"", "", TypeString, 0, ""},
		{"", "foo", TypeString, -1, ""},
		{"foo", "", TypeString, 1, ""},
		{"foo", "bar", TypeString, 1, ""},
		{"bar", "foo", TypeString, -1, ""},
		{"0", "0", TypeNumber, 0, ""},
		{"0", "0", TypeInteger, 0, ""},
	}

	for i, c := range cases {
		got, err := CompareTypeBytes([]byte(c.a), []byte(c.b), c.t)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if got != c.expect {
			t.Errorf("case %d response mismatch: %d != %d", i, c.expect, got)
			continue
		}
	}
}

func TestCompareIntegerBytes(t *testing.T) {
	cases := []struct {
		a, b   string
		expect int
		err    string
	}{
		{"0", "", 0, "strconv.ParseInt: parsing \"\": invalid syntax"},
		{"", "0", 0, "strconv.ParseInt: parsing \"\": invalid syntax"},
		{"0", "0", 0, ""},
		{"-1", "0", -1, ""},
		{"0", "-1", 1, ""},
	}

	for i, c := range cases {
		got, err := CompareIntegerBytes([]byte(c.a), []byte(c.b))
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if got != c.expect {
			t.Errorf("case %d response mismatch: %d != %d", i, c.expect, got)
			continue
		}
	}
}

func TestCompareNumberBytes(t *testing.T) {
	cases := []struct {
		a, b   string
		expect int
		err    string
	}{
		{"0", "", 0, "strconv.ParseFloat: parsing \"\": invalid syntax"},
		{"", "0", 0, "strconv.ParseFloat: parsing \"\": invalid syntax"},
		{"0", "0", 0, ""},
		{"-1", "0", -1, ""},
		{"0", "-1", 1, ""},
	}

	for i, c := range cases {
		got, err := CompareNumberBytes([]byte(c.a), []byte(c.b))
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if got != c.expect {
			t.Errorf("case %d response mismatch: %d != %d", i, c.expect, got)
			continue
		}
	}
}
