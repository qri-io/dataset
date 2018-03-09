// Package dsio defines writers & readers for operating on "container" data structures (objects and arrays)
package dsio

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

// EntryWriter is a generalized interface for writing structured data
type EntryWriter interface {
	// Structure gives the structure being written
	Structure() *dataset.Structure
	// WriteEntry writes one "row" of structured data to the Writer
	WriteEntry(Entry) error
	// Close finalizes the writer, indicating all entries
	// have been written
	Close() error
}

// EntryReader is a generalized interface for reading Ordered Structured Data
type EntryReader interface {
	// Structure gives the structure being read
	Structure() *dataset.Structure
	// ReadVal reads one row of structured data from the reader
	ReadEntry() (Entry, error)
}

// EntryReadWriter combines EntryWriter and EntryReader behaviors
type EntryReadWriter interface {
	// Structure gives the structure being read and written
	Structure() *dataset.Structure
	// ReadVal reads one row of structured data from the reader
	ReadEntry() (Entry, error)
	// WriteEntry writes one row of structured data to the ReadWriter
	WriteEntry(Entry) error
	// Close finalizes the ReadWriter, indicating all entries
	// have been written
	Close() error
	// Bytes gives the raw contents of the ReadWriter
	Bytes() []byte
}

// NewEntryReader allocates a EntryReader based on a given structure
func NewEntryReader(st *dataset.Structure, r io.Reader) (EntryReader, error) {
	switch st.Format {
	case dataset.CBORDataFormat:
		return NewCBORReader(st, r)
	case dataset.JSONDataFormat:
		return NewJSONReader(st, r)
	case dataset.CSVDataFormat:
		return NewCSVReader(st, r), nil
	case dataset.UnknownDataFormat:
		return nil, fmt.Errorf("structure must have a data format")
	default:
		return nil, fmt.Errorf("invalid format to create reader: %s", st.Format.String())
	}
}

// NewEntryWriter allocates a EntryWriter based on a given structure
func NewEntryWriter(st *dataset.Structure, w io.Writer) (EntryWriter, error) {
	switch st.Format {
	case dataset.CBORDataFormat:
		return NewCBORWriter(st, w)
	case dataset.JSONDataFormat:
		return NewJSONWriter(st, w)
	case dataset.CSVDataFormat:
		return NewCSVWriter(st, w), nil
	case dataset.UnknownDataFormat:
		return nil, fmt.Errorf("structure must have a data format")
	default:
		return nil, fmt.Errorf("invalid format to create writer: %s", st.Format.String())
	}
}

type scanMode int

const (
	smArray scanMode = iota
	smObject
)

// schemaScanMode determines weather the top level is an array or object
func schemaScanMode(sc *jsonschema.RootSchema) (scanMode, error) {
	if vt, ok := sc.Validators["type"]; ok {
		// TODO - lol go PR jsonschema to export access to this instead of this
		// silly validation hack
		obj := []jsonschema.ValError{}
		arr := []jsonschema.ValError{}
		vt.Validate("", map[string]interface{}{}, &obj)
		vt.Validate("", []interface{}{}, &arr)
		if len(obj) == 0 {
			return smObject, nil
		} else if len(arr) == 0 {
			return smArray, nil
		}
	}
	return smArray, fmt.Errorf("invalid schema. root must be either an array or object type")
}
