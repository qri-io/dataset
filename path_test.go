package dataset

import (
	"encoding/json"
	"testing"
)

func TestNewPath(t *testing.T) {
	cases := []struct {
		in, out []string
	}{
		{[]string{"handle.dataset", "dataset"}, []string{"handle", "dataset", "dataset"}},
		{[]string{"handle", "dataset", "dataset"}, []string{"handle", "dataset", "dataset"}},
		{[]string{"handle", "dataset", "dataset", "dataset", "dataset"}, []string{"handle", "dataset", "dataset", "dataset", "dataset"}},
		{[]string{"handle", "dataset", "dataset", "dataset", "dataset", "dataset"}, []string{"handle", "dataset", "dataset", "dataset", "dataset", "dataset"}},

		// {[]string{"handle", "", "table", "column", "row", "should_ignore"}, []string{"handle"}},
	}
	for i, c := range cases {
		out := NewPath(c.in...)
		for j, n := range out {
			if n != c.out[j] {
				t.Errorf("case %d slices don't match at index %d. expected: %s, got: %s", i, j, c.out, out)
				break
			}
		}
	}
}

func TestValidPathString(t *testing.T) {
	cases := []struct {
		str    string
		expect bool
	}{
		{"ns", true},
		{"handle.dataset_name", true},
		{"handle.dataset_name.", false},
		{"blah.blah", true},
		{"ns..", false},
		{"..", false},
		{".fjadksld.", false},
	}

	for i, c := range cases {
		if got := ValidPathString(c.str); got != c.expect {
			t.Errorf("case %d failed. %s should be %t", i, c.str, c.expect)
		}
	}
}

func TestPathMarshalJSON(t *testing.T) {
	cases := []struct {
		dt     Path
		expect string
		err    error
	}{
		{NewPath(""), "\"\"", nil},
		{NewPath("one", "two", "three", "four"), "\"one.two.three.four\"", nil},
		{NewPath("one.two.three.four"), "\"one.two.three.four\"", nil},
		// {PathModify, "\"MODIFY\"", nil},
		// {PathDelete, "\"DELETE\"", nil},
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

func TestPathUnmarshalJSON(t *testing.T) {
	cases := []struct {
		data []byte
		dt   Path
		err  error
	}{
		{[]byte("[\"\"]"), NewPath(""), nil},
		{[]byte("[\"one.two.three.four\"]"), NewPath("one.two.three.four"), nil},
		// {[]byte("[\"MODIFY\"]"), PathModify, nil},
		// {[]byte("[\"DELETE\"]"), PathDelete, nil},
	}

	for i, c := range cases {
		var dt []Path
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
