package dsio

import (
	"bytes"
	"encoding/json"
	"io"
	"time"

	"github.com/datatogether/cdxj"
	"github.com/qri-io/dataset"
)

// CDXJReader implements the RowReader interface for the CDXJ data format
type CDXJReader struct {
	st *dataset.Structure
	r  *cdxj.Reader
}

// NewCDXJReader allocates a reader from a structure and read source
func NewCDXJReader(st *dataset.Structure, r io.Reader) *CDXJReader {
	return &CDXJReader{
		st: st,
		r:  cdxj.NewReader(r),
	}
}

// Structure gives this reader's structure
func (r *CDXJReader) Structure() dataset.Structure {
	return *r.st
}

// ReadRow reads one CDXJ record from the reader
func (r *CDXJReader) ReadRow() ([][]byte, error) {
	rec, err := r.r.Read()
	if err != nil {
		return nil, err
	}

	u, err := cdxj.SurtURL(rec.URI)
	if err != nil {
		return nil, err
	}

	row := make([][]byte, 4)
	row[0] = []byte(u)
	row[1] = []byte(rec.Timestamp.Format(time.RFC3339))
	row[2] = []byte(rec.RecordType.String())
	row[3], err = json.Marshal(rec.JSON)
	if err != nil {
		return nil, err
	}
	return row, nil
}

// CDXJWriter implements the RowWriter interface for
// CDXJ-formatted data
type CDXJWriter struct {
	rowsWritten int
	st          *dataset.Structure
	w           *cdxj.Writer
}

// NewCDXJWriter creates a Writer from a structure and write destination
func NewCDXJWriter(st *dataset.Structure, w io.Writer) *CDXJWriter {
	writer := cdxj.NewWriter(w)
	return &CDXJWriter{
		st: st,
		w:  writer,
	}
}

// Structure gives this writer's structure
func (w *CDXJWriter) Structure() dataset.Structure {
	return *w.st
}

// WriteRow writes one CDXJ record to the writer
func (w *CDXJWriter) WriteRow(data [][]byte) error {
	r := &cdxj.Record{}
	joined := bytes.Join(data, []byte(" "))
	if err := r.UnmarshalCDXJ(joined); err != nil {
		return err
	}
	return w.WriteRecord(r)
}

// WriteRecord writes a cdxj record to the Writer
func (w *CDXJWriter) WriteRecord(rec *cdxj.Record) error {
	return w.w.Write(rec)
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *CDXJWriter) Close() error {
	return w.w.Close()
}
