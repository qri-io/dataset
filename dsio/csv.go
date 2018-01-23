package dsio

import (
	"encoding/csv"
	"github.com/qri-io/dataset"
	"io"
)

// CSVReader implements the RowReader interface for the CSV data format
type CSVReader struct {
	st         *dataset.Structure
	readHeader bool
	r          *csv.Reader
}

// NewCSVReader creates a reader from a structure and read source
func NewCSVReader(st *dataset.Structure, r io.Reader) *CSVReader {
	return &CSVReader{
		st: st,
		r:  csv.NewReader(r),
	}
}

// Structure gives this reader's structure
func (r *CSVReader) Structure() *dataset.Structure {
	return r.st
}

// ReadRow reads one CSV record from the reader
func (r *CSVReader) ReadRow() ([][]byte, error) {
	if !r.readHeader {
		if HasHeaderRow(r.st) {
			if _, err := r.r.Read(); err != nil {
				if err.Error() == "EOF" {
					return nil, nil
				}
				return nil, err
			}
		}
		r.readHeader = true
	}

	data, err := r.r.Read()
	if err != nil {
		return nil, err
	}
	row := make([][]byte, len(data))
	for i, d := range data {
		row[i] = []byte(d)
	}
	return row, nil
}

// HasHeaderRow checks Structure for the presence of the HeaderRow flag
func HasHeaderRow(st *dataset.Structure) bool {
	if st.Format == dataset.CSVDataFormat && st.FormatConfig != nil {
		if csvOpt, ok := st.FormatConfig.(*dataset.CSVOptions); ok {
			return csvOpt.HeaderRow
		}
	}
	return false
}

// CSVWriter implements the RowWriter interface for
// CSV-formatted data
type CSVWriter struct {
	rowsWritten int
	w           *csv.Writer
	st          *dataset.Structure
}

// NewCSVWriter creates a Writer from a structure and write destination
func NewCSVWriter(st *dataset.Structure, w io.Writer) *CSVWriter {
	writer := csv.NewWriter(w)
	wr := &CSVWriter{
		st: st,
		w:  writer,
	}

	if CSVOpts, ok := st.FormatConfig.(*dataset.CSVOptions); ok {
		if CSVOpts.HeaderRow {
			// TODO - capture error
			// TODO - restore
			// writer.Write(st.Schema.FieldNames())
		}
	}

	return wr
}

// Structure gives this writer's structure
func (w *CSVWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteRow writes one CSV record to the writer
func (w *CSVWriter) WriteRow(data [][]byte) error {
	row := make([]string, len(data))
	for i, d := range data {
		row[i] = string(d)
	}
	return w.w.Write(row)
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *CSVWriter) Close() error {
	w.w.Flush()
	return nil
}
