package validate

import (
	"testing"
)

func TestValidName(t *testing.T) {
	cases := []struct {
		name string
		err  string
	}{
		{"", "error: name cannot be empty"},
		{"9", "error: illegal name '9', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters"},
		{"_", "error: illegal name '_', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters"},
		{"_foo", "error: illegal name '_foo', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters"},
	}

	for i, c := range cases {
		err := ValidName(c.name)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
	}
}
