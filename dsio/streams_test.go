package dsio

import (
	"bytes"
	"strings"
	"testing"

	"github.com/qri-io/dataset"
)

func TestCopyJSONToJSON(t *testing.T) {
	text := "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4}]"
	expected := text
	sink := bytes.NewBufferString("")
	st := &dataset.Structure{
		Format: "json",
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewJSONWriter(st, sink)
	if err != nil {
		t.Fatal(err)
	}
	err = Copy(r, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	str := sink.String()
	if str != expected {
		t.Errorf("Copy from json to json did not succeed: %v <> %v", str, expected)
	}
}

func TestCopyJSONToBytes(t *testing.T) {
	text := "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4}]"
	expected := []byte{91, 123, 34, 97, 34, 58, 49, 125, 44, 123, 34, 98, 34, 58, 50, 125, 44, 123, 34, 99, 34, 58, 51, 125, 44, 123, 34, 100, 34, 58, 52, 125, 93}
	st := &dataset.Structure{
		Format: "json",
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewEntryBuffer(st)
	if err != nil {
		t.Fatal(err)
	}
	err = Copy(r, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	b := w.Bytes()
	if bytes.Compare(b, expected) != 0 {
		t.Errorf("Copy from json to bytes did not succeed: %v <> %v", b, expected)
	}
}

func TestCopyJSONToCBOR(t *testing.T) {
	text := "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4}]"
	expected := []byte{132, 161, 97, 97, 1, 161, 97, 98, 2, 161, 97, 99, 3, 161, 97, 100, 4}
	sink := bytes.Buffer{}
	st := &dataset.Structure{
		Format: "json",
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewCBORWriter(st, &sink)
	if err != nil {
		t.Fatal(err)
	}
	err = Copy(r, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	b := sink.Bytes()
	if bytes.Compare(b, expected) != 0 {
		t.Errorf("Copy from json to cbor did not succeed: %v <> %v", b, expected)
	}
}

func TestCopyJSONToJSONWithPaging(t *testing.T) {
	text := "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4}]"
	expected := "[{\"b\":2},{\"c\":3}]"
	sink := bytes.NewBufferString("")
	st := &dataset.Structure{
		Format: "json",
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewJSONWriter(st, sink)
	if err != nil {
		t.Fatal(err)
	}
	p := &PagedReader{Reader: r, Limit: 2, Offset: 1}
	err = Copy(p, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	str := sink.String()
	if str != expected {
		t.Errorf("Copy with paging did not succeed: %v <> %v", str, expected)
	}
}

func TestCopyJSONToJSONPagingRunsOut(t *testing.T) {
	text := "[{\"a\":1},{\"b\":2}]"
	expected := "[{\"b\":2}]"
	sink := bytes.NewBufferString("")
	st := &dataset.Structure{
		Format: "json",
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewJSONWriter(st, sink)
	if err != nil {
		t.Fatal(err)
	}
	p := &PagedReader{Reader: r, Limit: 2, Offset: 1}
	err = Copy(p, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	str := sink.String()
	if str != expected {
		t.Errorf("Copy limited due to paging did not succeed: %v <> %v", str, expected)
	}
}

// We've had lots of problems with qri eating csv header rows in other parts of the
// codebase. This is less silly than it looks
func TestCopyCSVToCSV(t *testing.T) {
	text := `title,count,is great
foo,1,true
bar,2,false
bat,3,meh
`
	sink := bytes.NewBufferString("")
	st := &dataset.Structure{
		Format: "csv",
		FormatConfig: map[string]interface{}{
			"headerRow": true,
		},
		Schema: map[string]interface{}{
			"type": "array",
			"items": map[string]interface{}{
				"type": "array",
				"items": []interface{}{
					map[string]interface{}{"title": "title", "type": "string"},
					map[string]interface{}{"title": "count", "type": "integer"},
					map[string]interface{}{"title": "is great", "type": "string"},
				},
			},
		},
	}

	r, err := NewEntryReader(st, strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewEntryWriter(st, sink)
	if err != nil {
		t.Fatal(err)
	}

	err = Copy(r, w)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	got := sink.String()
	if text != got {
		t.Errorf("result mismatch. expected: '%s'\ngot: '%s'", text, got)
	}
}
