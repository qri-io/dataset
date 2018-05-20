package detect

import (
	"io"
	"strings"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

func TestJSONSchema(t *testing.T) {

	pr, _ := io.Pipe()
	pr.Close()
	_, _, err := JSONSchema(&dataset.Structure{}, pr)
	if err == nil {
		t.Error("expected error when reading bad reader")
		return
	}

	cases := []struct {
		st     *dataset.Structure
		data   string
		expect *jsonschema.RootSchema
		err    string
	}{
		{&dataset.Structure{}, "", nil, "invalid json data"},
		{&dataset.Structure{}, "f", nil, "invalid json data"},
		{&dataset.Structure{}, "{", dataset.BaseSchemaObject, ""},
		{&dataset.Structure{}, "[", dataset.BaseSchemaArray, ""},
		{&dataset.Structure{}, strings.Repeat(" ", 250) + "[", dataset.BaseSchemaArray, ""},
	}

	for i, c := range cases {
		rdr := strings.NewReader(c.data)

		got, _, err := JSONSchema(c.st, rdr)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			return
		}

		// TODO - this is just basic pointer comparison for now,
		// if JSONSchema ever returns a fresh schema this'll have to be improved
		if got != c.expect {
			t.Errorf("case %d return mismatch", i)
		}
	}
}
