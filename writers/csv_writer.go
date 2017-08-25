package writers

import (
	"bytes"
	"encoding/csv"
	"github.com/qri-io/dataset"
)

type CsvWriter struct {
	rowsWritten int
	w           *csv.Writer
	st          *dataset.Structure
	buf         *bytes.Buffer
}

func NewCsvWriter(st *dataset.Structure) *CsvWriter {
	buf := bytes.NewBuffer(nil)
	writer := csv.NewWriter(buf)
	return &CsvWriter{
		st:  st,
		w:   writer,
		buf: buf,
	}
}

func (w *CsvWriter) WriteRow(data [][]byte) error {
	row := make([]string, len(data))
	for i, d := range data {
		row[i] = string(d)
	}

	return w.w.Write(row)
}

func (w *CsvWriter) Close() error {
	w.w.Flush()
	// no-op to satisfy interface
	return nil
}

func (w *CsvWriter) Bytes() []byte {
	return w.buf.Bytes()
}
