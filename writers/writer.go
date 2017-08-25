package writers

import (
	"github.com/qri-io/dataset"
)

type Writer interface {
	WriteRow(row [][]byte) error
	Close() error
	Bytes() []byte
}

func NewWriter(st *dataset.Structure) Writer {
	switch st.Format {
	case dataset.CsvDataFormat:
		return NewCsvWriter(st)
	case dataset.JsonArrayDataFormat:
		return NewJsonWriter(st, false)
	case dataset.JsonDataFormat:
		return NewJsonWriter(st, true)
	default:
		// TODO - should this error or something?
		return NewCsvWriter(st)
	}
}
