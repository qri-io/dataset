package detect

import (
	"io"
	"strings"
	"testing"

	"github.com/qri-io/dataset"
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
		expect map[string]interface{}
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

		if err := dataset.CompareSchemas(got, c.expect); err != nil {
			t.Errorf("case %d returned schema mismatch: %s", i, err)
		}
	}
}
