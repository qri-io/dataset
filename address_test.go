package dataset

import (
	"encoding/json"
	"testing"
)

func TestNewAddress(t *testing.T) {
	cases := []struct {
		in, out []string
	}{
		{[]string{"", "handle.dataset", "dataset"}, []string{"handle", "dataset", "dataset"}},
		{[]string{"handle.dataset", "dataset"}, []string{"handle", "dataset", "dataset"}},
		{[]string{"handle", "dataset", "dataset"}, []string{"handle", "dataset", "dataset"}},
		{[]string{"handle", "dataset", "dataset", "dataset", "dataset"}, []string{"handle", "dataset", "dataset", "dataset", "dataset"}},
		{[]string{"handle", "dataset", "dataset", "dataset", "dataset", "dataset"}, []string{"handle", "dataset", "dataset", "dataset", "dataset", "dataset"}},

		// {[]string{"handle", "", "table", "column", "row", "should_ignore"}, []string{"handle"}},
	}
	for i, c := range cases {
		out := NewAddress(c.in...)
		for j, n := range out {
			if n != c.out[j] {
				t.Errorf("case %d slices don't match at index %d. expected: %s, got: %s", i, j, c.out, out)
				break
			}
		}
	}
}

func TestPathString(t *testing.T) {
	cases := []struct {
		adr, in []string
		out     string
	}{
		{[]string{"a", "b", "c"}, []string{""}, "/a/b/c/"},
		{[]string{"a", "b", "c"}, []string{"d.csv"}, "/a/b/c/d.csv"},
		{[]string{"a", "b", "c"}, []string{"d", "e.markup.markdown"}, "/a/b/c/d/e.markup.markdown"},
	}

	for i, c := range cases {
		got := NewAddress(c.adr...).PathString(c.in...)
		if got != c.out {
			t.Errorf("case %d path mismatch. expected: %s, got: %s", i, c.out, got)
		}
	}
}

func TestAddressParent(t *testing.T) {
	cases := []struct {
		str, expect string
	}{
		{"ns", ""},
		{"ns.stuff", "ns"},
		{"ns.stuff.and", "ns.stuff"},
	}

	for i, c := range cases {
		if got := NewAddress(c.str).Parent(); got.String() != c.expect {
			t.Errorf("case %d failed. %s should equal %s", i, got.String(), c.expect)
		}
	}
}

func TestAddressIsParent(t *testing.T) {
	cases := []struct {
		a, b   string
		expect bool
	}{
		{"", "", false},
		{"ns", "", false},
		{"ns", "ns.a", true},
		{"ns", "bs.a", false},
		{"ns", "ns.a.b", false},
		{"ns.a", "ns.a", false},
	}

	for i, c := range cases {
		if got := NewAddress(c.a).IsParent(NewAddress(c.b)); got != c.expect {
			t.Errorf("case %d failted. '%s' isParent '%s' should equal %t", i, c.a, c.b, c.expect)
		}
	}
}

func TestAddressIsAncestor(t *testing.T) {
	cases := []struct {
		a, b   string
		expect bool
	}{
		{"", "", false},
		{"ns", "", false},
		{"ns", "ns.a", true},
		{"ns", "bs.a", false},
		{"ns", "ns.a.b", true},
		{"ns", "bs.a.b", false},
		{"", "bs.a.b", true},
	}
	for i, c := range cases {
		if got := NewAddress(c.a).IsAncestor(NewAddress(c.b)); got != c.expect {
			t.Errorf("case %d failted. '%s' isAncestor '%s' should equal %t", i, c.a, c.b, c.expect)
		}
	}
}

func TestValidAddressString(t *testing.T) {
	cases := []struct {
		str    string
		expect bool
	}{
		// TODO
		// {"ns", true},
		{"1ns", false},
		{"handle.dataset_name", true},
		{"qri.b.epasearch", true},
		{"qri.b5.epasearch", true},
		{"qri.b5.epa_search", true},
		{"handle.dataset_name.", false},
		{"blah.blah", true},
		{"ns..", false},
		// TODO
		// {"ns..ster", false},
		{"..", false},
		{".fjadksld.", false},
	}

	for i, c := range cases {
		if got := ValidAddressString(c.str); got != c.expect {
			t.Errorf("case %d failed. %s should be %t", i, c.str, c.expect)
		}
	}
}

func TestAddressMarshalJSON(t *testing.T) {
	cases := []struct {
		dt     Address
		expect string
		err    error
	}{
		{NewAddress(""), "\"\"", nil},
		{NewAddress("one", "two", "three", "four"), "\"one.two.three.four\"", nil},
		{NewAddress("one.two.three.four"), "\"one.two.three.four\"", nil},
		// {AddressModify, "\"MODIFY\"", nil},
		// {AddressDelete, "\"DELETE\"", nil},
	}

	for i, c := range cases {
		got, err := json.Marshal(c.dt)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
		}
		if string(got) != c.expect {
			t.Errorf("case %d byte mismatch. expected: %s, got: %s", i, c.expect, string(got))
		}
	}
}

func TestAddressUnmarshalJSON(t *testing.T) {
	cases := []struct {
		data []byte
		dt   Address
		err  error
	}{
		{[]byte("[\"\"]"), NewAddress(""), nil},
		{[]byte("[\"one.two.three.four\"]"), NewAddress("one.two.three.four"), nil},
		// {[]byte("[\"MODIFY\"]"), AddressModify, nil},
		// {[]byte("[\"DELETE\"]"), AddressDelete, nil},
	}

	for i, c := range cases {
		var dt []Address
		err := json.Unmarshal(c.data, &dt)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		// d := dt[0]
		// if c.dt != d {
		// 	t.Errorf("case %d byte mismatch. expected: %s, got: %s", i, c.dt, d)
		// }
	}
}
