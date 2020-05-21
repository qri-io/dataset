package validate

import (
	"fmt"
	"testing"

	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/dstest"
)

func TestEntryReader(t *testing.T) {
	cases := []struct {
		name   string
		err    string
		errors []string
	}{
		{"craigslist", "", nil},
		{"movies", "", []string{
			`/0/1: "" type should be integer, got string`,
			`/1/1: "" type should be integer, got string`,
		}},
	}

	for _, c := range cases {
		tc, err := dstest.NewTestCaseFromDir(fmt.Sprintf("testdata/%s", c.name))
		if err != nil {
			t.Errorf("%s: error loading %s", c.name, err.Error())
			continue
		}

		r, err := dsio.NewEntryReader(tc.Input.Structure, tc.BodyFile())
		if err != nil {
			t.Errorf("%s: error creating entry reader: %s", c.name, err.Error())
			continue
		}

		errors, err := EntryReader(r)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("%s error mismatch. expected: %s, got: %s", c.name, c.err, err)
			continue
		}

		if len(errors) != len(c.errors) {
			t.Errorf("%s: error length mismatch. expected: %d, got: %d", c.name, len(c.errors), len(errors))
			continue
		}

		for j, e := range errors {
			if e.Error() != c.errors[j] {
				t.Errorf("%s: validation error %d mismatch. expected: %s, got: %s", c.name, j, c.errors[j], e.Error())
				continue
			}
		}
	}
}
