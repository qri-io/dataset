// Package dsio defines writers & readers for dataset data
package dsio

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset"
)

// RowWriter is a generalized interface for writing structured data
type RowWriter interface {
	// Structure gives the structure being written
	Structure() dataset.Structure
	// WriteRow writes one row of structured data to the Writer
	WriteRow(row [][]byte) error
	// Close finalizes the writer, indicating all entries
	// have been written
	Close() error
}

// RowReader is a generalized interface for reading Structured Data
type RowReader interface {
	// Structure gives the structure being read
	Structure() dataset.Structure
	// ReadRow reads one row of structured data from the reader
	ReadRow() ([][]byte, error)
}

// RowReadWriter combines RowWriter and RowReader behaviors
type RowReadWriter interface {
	// Structure gives the structure being read and written
	Structure() dataset.Structure
	// ReadRow reads one row of structured data from the reader
	ReadRow() ([][]byte, error)
	// WriteRow writes one row of structured data to the ReadWriter
	WriteRow(row [][]byte) error
	// Close finalizes the ReadWriter, indicating all entries
	// have been written
	Close() error
	// Bytes gives the raw contents of the ReadWriter
	Bytes() []byte
}

// NewRowReader allocates a RowReader based on a given structure
func NewRowReader(st *dataset.Structure, r io.Reader) (RowReader, error) {
	switch st.Format {
	case dataset.CSVDataFormat:
		return NewCSVReader(st, r), nil
	case dataset.JSONDataFormat:
		return NewJSONReader(st, r), nil
	case dataset.CDXJDataFormat:
		return NewCDXJReader(st, r), nil
	case dataset.UnknownDataFormat:
		return nil, fmt.Errorf("structure must have a data format")
	default:
		return nil, fmt.Errorf("invalid format to create reader: %s", st.Format.String())
	}
}

// NewRowWriter allocates a RowWriter based on a given structure
func NewRowWriter(st *dataset.Structure, w io.Writer) (RowWriter, error) {
	switch st.Format {
	case dataset.CSVDataFormat:
		return NewCSVWriter(st, w), nil
	case dataset.JSONDataFormat:
		return NewJSONWriter(st, w), nil
	case dataset.CDXJDataFormat:
		return NewCDXJWriter(st, w), nil
	case dataset.UnknownDataFormat:
		return nil, fmt.Errorf("structure must have a data format")
	default:
		return nil, fmt.Errorf("invalid format to create writer: %s", st.Format.String())
	}
}
