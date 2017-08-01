package dataset

import (
	"testing"
)

func TestHashBytes(t *testing.T) {
	cases := []struct {
		in  []byte
		out string
		err error
	}{
		{[]byte(""), "1220e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", nil},
	}

	for i, c := range cases {
		got, err := HashBytes(c.in)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s got: %s", i, c.err, err)
			continue
		}

		if got != c.out {
			t.Errorf("case %d result mismatch. expected: %s got: %s", i, c.out, got)
			continue
		}
	}
}
