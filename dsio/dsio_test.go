package dsio

import (
	"bytes"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
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

func TestReadAll(t *testing.T) {
	if _, err := ReadAll(&JSONReader{st: &dataset.Structure{}}); err == nil {
		t.Error("expected malformed json reader read-all to fail")
	}

	buf := bytes.NewBuffer([]byte(csvData))
	arrayReader, err := NewEntryReader(csvStruct, buf)
	if err != nil {
		t.Errorf("error allocating EntryReader: %s", err.Error())
		return
	}

	got, err := ReadAll(arrayReader)
	if err != nil {
		t.Fatal(err)
	}

	expectArr := []interface{}{
		[]interface{}{
			"a",
			float64(1.23),
			int64(4),
			bool(false),
			map[string]interface{}{"a": "b"},
			[]interface{}{float64(1), float64(2), float64(3)},
			nil,
		},
		[]interface{}{
			"a",
			float64(1.23),
			int64(4),
			bool(false),
			map[string]interface{}{"a": "b"},
			[]interface{}{float64(1), float64(2), float64(3)},
			nil,
		},
		[]interface{}{
			"a",
			float64(1.23),
			int64(4),
			bool(false),
			map[string]interface{}{"a": "b"},
			[]interface{}{float64(1), float64(2), float64(3)},
			nil,
		},
		[]interface{}{
			"a",
			float64(1.23),
			int64(4),
			bool(false),
			map[string]interface{}{"a": "b"},
			[]interface{}{float64(1), float64(2), float64(3)},
			nil,
		},
		[]interface{}{
			"a",
			float64(1.23),
			int64(4),
			bool(false),
			map[string]interface{}{"a": "b"},
			[]interface{}{float64(1), float64(2), float64(3)},
			nil,
		},
	}
	if diff := cmp.Diff(expectArr, got); diff != "" {
		t.Errorf("arry result mismatch (-want +got):\n%s", diff)
	}

	objReader, err := NewJSONReader(&dataset.Structure{Format: "json", Schema: dataset.BaseSchemaObject}, strings.NewReader(`{"a":"1","b":0,"c":false}`))
	got, err = ReadAll(objReader)
	if err != nil {
		t.Fatal(err)
	}

	expectObj := map[string]interface{}{
		"a": "1",
		"b": int64(0),
		"c": false,
	}
	if diff := cmp.Diff(expectObj, got); diff != "" {
		t.Errorf("object result mismatch (-want +got):\n%s", diff)
	}
}
