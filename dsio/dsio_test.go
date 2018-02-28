package dsio

import (
	"bytes"
	"testing"

	"github.com/qri-io/dataset"
)

func TestNewValueReader(t *testing.T) {
	cases := []struct {
		st  *dataset.Structure
		err string
	}{
		{&dataset.Structure{}, "structure must have a data format"},
		{&dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: dataset.CSVDataFormat, Schema: dataset.BaseSchemaArray}, ""},
	}

	for i, c := range cases {
		_, err := NewValueReader(c.st, &bytes.Buffer{})
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestNewValueWriter(t *testing.T) {
	cases := []struct {
		st  *dataset.Structure
		err string
	}{
		{&dataset.Structure{}, "structure must have a data format"},
		{&dataset.Structure{Format: dataset.CBORDataFormat, Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: dataset.CSVDataFormat, Schema: dataset.BaseSchemaArray}, ""},
	}

	for i, c := range cases {
		_, err := NewValueWriter(c.st, &bytes.Buffer{})
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}
