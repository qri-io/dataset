// Package dsio defines writers & readers for operating on "container" data structures (objects and arrays)
package dsio

import (
	"fmt"
	"io"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
)

var log = logger.Logger("dsio")

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
	// Close finalizes the Reader
	Close() error
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
	switch st.DataFormat() {
	case dataset.CBORDataFormat:
		return NewCBORReader(st, r)
	case dataset.JSONDataFormat:
		return NewJSONReader(st, r)
	case dataset.CSVDataFormat:
		return NewCSVReader(st, r)
	case dataset.XLSXDataFormat:
		return NewXLSXReader(st, r)
	case dataset.UnknownDataFormat:
		err := fmt.Errorf("structure must have a data format")
		log.Debug(err.Error())
		return nil, err
	default:
		err := fmt.Errorf("invalid format to create reader: %s", st.Format)
		log.Debug(err.Error())
		return nil, err
	}
}

// NewEntryWriter allocates a EntryWriter based on a given structure
func NewEntryWriter(st *dataset.Structure, w io.Writer) (EntryWriter, error) {
	switch st.DataFormat() {
	case dataset.CBORDataFormat:
		return NewCBORWriter(st, w)
	case dataset.JSONDataFormat:
		return NewJSONWriter(st, w)
	case dataset.CSVDataFormat:
		return NewCSVWriter(st, w)
	case dataset.XLSXDataFormat:
		return NewXLSXWriter(st, w)
	case dataset.UnknownDataFormat:
		err := fmt.Errorf("structure must have a data format")
		log.Debug(err.Error())
		return nil, err
	default:
		err := fmt.Errorf("invalid format to create writer: %s", st.Format)
		log.Debug(err.Error())
		return nil, err
	}
}

// GetTopLevelType returns the top-level type of the structure, only if it is
// a valid type ("array" or "object"), otherwise returns an error
func GetTopLevelType(st *dataset.Structure) (string, error) {
	// tlt := st.Schema.TopLevelType()
	if st.Schema == nil {
		return "", fmt.Errorf("a schema object is required")
	}
	tlt, ok := st.Schema["type"].(string)
	if !ok {
		return "", fmt.Errorf("schema top level 'type' value must be either 'array' or 'object'")
	}
	if tlt != "array" && tlt != "object" {
		return "", fmt.Errorf("invalid schema. root must be either an array or object type")
	}
	return tlt, nil
}

// ReadAll consumes an EntryReader, returning it's values
func ReadAll(r EntryReader) (interface{}, error) {
	tlt, err := GetTopLevelType(r.Structure())
	if err != nil {
		return nil, err
	}

	if tlt == "object" {
		return ReadAllObject(r)
	}
	return ReadAllArray(r)
}

// ReadAllObject consumes an EntryReader with an "object" top level type,
// returning a map[string]interface{} of values
func ReadAllObject(r EntryReader) (map[string]interface{}, error) {
	obj := make(map[string]interface{})
	for i := 0; ; i++ {
		val, err := r.ReadEntry()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		obj[val.Key] = val.Value
	}
	return obj, nil
}

// ReadAllArray consumes an EntryReader with an "array" top level type,
// returning a map[string]interface{} of values
func ReadAllArray(r EntryReader) ([]interface{}, error) {
	array := make([]interface{}, 0)
	for i := 0; ; i++ {
		val, err := r.ReadEntry()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		array = append(array, val.Value)
	}
	return array, nil
}
