package dsio

import (
	"fmt"
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/qri-io/dataset"
)

/*
func TestCopyJSONToJSON(t *testing.T) {
	text := "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4}]"
	expected := text
	sink := bytes.NewBufferString("")
	st := &dataset.Structure{
		Format: dataset.JSONDataFormat,
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Error(err)
		return
	}
	w, err := NewJSONWriter(st, sink)
	if err != nil {
		t.Error(err)
		return
	}
	err = Copy(r, w)
	if err != nil {
		t.Error(err)
		return
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
		Format: dataset.JSONDataFormat,
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Error(err)
		return
	}
	w, err := NewEntryBuffer(st)
	if err != nil {
		t.Error(err)
		return
	}
	err = Copy(r, w)
	if err != nil {
		t.Error(err)
		return
	}
	w.Close()
	b := w.Bytes()
	if bytes.Compare(b, expected) != 0 {
		t.Errorf("Copy from json to bytes did not succeed: %v <> %v", b, expected)
	}
}
*/
func TestCopyJSONToCBOR(t *testing.T) {
	fmt.Printf("*** TestCopyJSONToCBOR\n")

	text := "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4}]"
	expected := []byte{132, 161, 97, 97, 1, 161, 97, 98, 2, 161, 97, 99, 3, 161, 97, 100, 4}
	var b bytes.Buffer
	sink := bufio.NewWriter(&b)
	st := &dataset.Structure{
		Format: dataset.JSONDataFormat,
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Error(err)
		return
	}
	w, err := NewCBORWriter(st, sink)
	if err != nil {
		t.Error(err)
		return
	}
	err = Copy(r, w)
	if err != nil {
		t.Error(err)
		return
	}
	w.Close()
	if bytes.Compare(b.Bytes(), expected) != 0 {
		t.Errorf("Copy from json to cbor did not succeed: %v <> %v", b.Bytes(), expected)
	}
}
/*
func TestCopyJSONToJSONWithPaging(t *testing.T) {
	text := "[{\"a\":1},{\"b\":2},{\"c\":3},{\"d\":4}]"
	expected := "[{\"b\":2},{\"c\":3}]"
	sink := bytes.NewBufferString("")
	st := &dataset.Structure{
		Format: dataset.JSONDataFormat,
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Error(err)
		return
	}
	w, err := NewJSONWriter(st, sink)
	if err != nil {
		t.Error(err)
		return
	}
	p := &PagedReader{Reader: r, Limit: 2, Offset: 1}
	err = Copy(p, w)
	if err != nil {
		t.Error(err)
		return
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
		Format: dataset.JSONDataFormat,
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewJSONReader(st, strings.NewReader(text))
	if err != nil {
		t.Error(err)
		return
	}
	w, err := NewJSONWriter(st, sink)
	if err != nil {
		t.Error(err)
		return
	}
	p := &PagedReader{Reader: r, Limit: 2, Offset: 1}
	err = Copy(p, w)
	if err != nil {
		t.Error(err)
		return
	}
	w.Close()
	str := sink.String()
	if str != expected {
		t.Errorf("Copy limited due to paging did not succeed: %v <> %v", str, expected)
	}
}
*/
