package dsio

import (
	"bytes"
	"testing"

	"github.com/qri-io/dataset"
)

var basicTableSchema = map[string]interface{}{
	"type": "array",
	"items": map[string]interface{}{
		"type": "array",
		"items": []interface{}{
			map[string]interface{}{"title": "column_one", "type": "string"},
		},
	},
}

func TestNewEntryReader(t *testing.T) {
	cases := []struct {
		st  *dataset.Structure
		err string
	}{
		{&dataset.Structure{}, "structure must have a data format"},
		{&dataset.Structure{Format: "cbor", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "csv", Schema: basicTableSchema}, ""},
		// {&dataset.Structure{Format: "xlsx", Schema: basicTableSchema}, ""},
	}

	for i, c := range cases {
		_, err := NewEntryReader(c.st, &bytes.Buffer{})
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestNewEntryWriter(t *testing.T) {
	cases := []struct {
		st  *dataset.Structure
		err string
	}{
		{&dataset.Structure{}, "structure must have a data format"},
		{&dataset.Structure{Format: "cbor", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "csv", Schema: basicTableSchema}, ""},
		// {&dataset.Structure{Format: "xlsx", Schema: basicTableSchema}, ""},
	}

	for i, c := range cases {
		_, err := NewEntryWriter(c.st, &bytes.Buffer{})
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}
