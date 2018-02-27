// Package dsio defines writers & readers for operating on "container" data structures (objects and arrays)
package dsio

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
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
	case dataset.CSVDataFormat:
		return NewCSVReader(st, r), nil
	case dataset.JSONDataFormat:
		return NewJSONReader(st, r)
	case dataset.UnknownDataFormat:
		return nil, fmt.Errorf("structure must have a data format")
	default:
		return nil, fmt.Errorf("invalid format to create reader: %s", st.Format.String())
	}
}

// NewValueWriter allocates a ValueWriter based on a given structure
func NewValueWriter(st *dataset.Structure, w io.Writer) (ValueWriter, error) {
	switch st.Format {
	case dataset.CSVDataFormat:
		return NewCSVWriter(st, w), nil
	case dataset.JSONDataFormat:
		return NewJSONWriter(st, w)
	case dataset.UnknownDataFormat:
		return nil, fmt.Errorf("structure must have a data format")
	default:
		return nil, fmt.Errorf("invalid format to create writer: %s", st.Format.String())
	}
}
