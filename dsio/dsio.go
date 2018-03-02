// Package dsio defines writers & readers for operating on "container" data structures (objects and arrays)
package dsio

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
	"github.com/qri-io/jsonschema"
)

// ValueWriter is a generalized interface for writing structured data
type ValueWriter interface {
	// Structure gives the structure being written
	Structure() *dataset.Structure
	// WriteValue writes one row of structured data to the Writer
	WriteValue(val vals.Value) error
	// Close finalizes the writer, indicating all entries
	// have been written
	Close() error
}

// ValueReader is a generalized interface for reading Ordered Structured Data
type ValueReader interface {
	// Structure gives the structure being read
	Structure() *dataset.Structure
	// ReadVal reads one row of structured data from the reader
	ReadValue() (vals.Value, error)
}

// ValueReadWriter combines ValueWriter and ValueReader behaviors
type ValueReadWriter interface {
	// Structure gives the structure being read and written
	Structure() *dataset.Structure
	// ReadVal reads one row of structured data from the reader
	ReadValue() (vals.Value, error)
	// WriteValue writes one row of structured data to the ReadWriter
	WriteValue(val vals.Value) error
	// Close finalizes the ReadWriter, indicating all entries
	// have been written
	Close() error
	// Bytes gives the raw contents of the ReadWriter
	Bytes() []byte
}

// NewValueReader allocates a ValueReader based on a given structure
func NewValueReader(st *dataset.Structure, r io.Reader) (ValueReader, error) {
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

// NewValueWriter allocates a ValueWriter based on a given structure
func NewValueWriter(st *dataset.Structure, w io.Writer) (ValueWriter, error) {
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
		if errs := vt.Validate(map[string]interface{}{}); len(errs) == 0 {
			return smObject, nil
		} else if errs := vt.Validate([]interface{}{}); len(errs) == 0 {
			return smArray, nil
		}
	}
	return smArray, fmt.Errorf("invalid schema. root must be either an array or object type")
}
