package dataset

import (
	"encoding/json"
	"testing"
)

func TestKindValid(t *testing.T) {
	cases := []struct {
		Kind Kind
		err  string
	}{
		{"", "invalid kind: ''. kind must be in the form qri:[type]:[version]"},
		{"qri:ds:0", ""},
		{"qri:st:0", ""},
		{"qri:as:0", ""},
		{"qri:ps:0", ""},
		{"qri:ps:0", ""},
	}

	for i, c := range cases {
		err := c.Kind.Valid()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestKindDatatype(t *testing.T) {
	cases := []struct {
		Kind   Kind
		expect string
	}{
		{"qri:ds:0", "ds"},
		{"qri:st:0", "st"},
		{"qri:as:0", "as"},
		{"qri:ps:0", "ps"},
	}

	for i, c := range cases {
		got := c.Kind.Type()
		if c.expect != got {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.expect, got)
			continue
		}
	}
}

func TestKindVersion(t *testing.T) {
	cases := []struct {
		Kind   Kind
		expect string
	}{
		{"qri:st:2", "2"},
		{"qri:ds:23", "23"},
	}

	for i, c := range cases {
		got := c.Kind.Version()
		if c.expect != got {
			t.Errorf("case %d response mismatch. expected: '%s', got: '%s'", i, c.expect, got)
			continue
		}
	}
}

func TestKindUnmarshalJSON(t *testing.T) {
	cases := []struct {
		input  string
		expect Kind
		err    string
	}{
		{`"qri:st:2"`, Kind("qri:st:2"), ""},
		{`""`, Kind(""), "invalid kind: ''. kind must be in the form qri:[type]:[version]"},
	}

	for i, c := range cases {
		got := Kind("")
		err := json.Unmarshal([]byte(c.input), &got)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if got != c.expect {
			t.Errorf("case %d response mismatch. expected: '%s', got: '%s'", i, c.expect, got)
		}
	}
}
