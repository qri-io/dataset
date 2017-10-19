package dsio

import (
	"io"
	"strconv"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/datatypes"
)

type JsonWriter struct {
	writeObjects bool
	rowsWritten  int
	ds           *dataset.Structure
	wr           io.Writer
}

func NewJsonWriter(ds *dataset.Structure, w io.Writer, writeObjects bool) *JsonWriter {
	return &JsonWriter{
		writeObjects: writeObjects,
		ds:           ds,
		wr:           w,
	}
}

func (w *JsonWriter) WriteRow(row [][]byte) error {
	if w.rowsWritten == 0 {
		if _, err := w.wr.Write([]byte{'['}); err != nil {
			return err
		}
	}

	if w.writeObjects {
		return w.writeObjectRow(row)
	}
	return w.writeArrayRow(row)
}

func (w *JsonWriter) writeObjectRow(row [][]byte) error {
	enc := []byte{',', '\n', '{'}
	if w.rowsWritten == 0 {
		enc = enc[1:]
	}
	for i, c := range row {
		f := w.ds.Schema.Fields[i]
		ent := []byte(",\"" + f.Name + "\":")
		if i == 0 {
			ent = ent[1:]
		}
		if c == nil || len(c) == 0 {
			ent = append(ent, []byte("null")...)
		} else {
			switch f.Type {
			case datatypes.String:
				ent = append(ent, []byte(strconv.Quote(string(c)))...)
			case datatypes.Float, datatypes.Integer:
				// if len(c) == 0 {
				// 	ent = append(ent, []byte("null")...)
				// } else {
				// 	ent = append(ent, c...)
				// }
				ent = append(ent, c...)
			case datatypes.Boolean:
				// TODO - coerce to true & false specifically
				ent = append(ent, c...)
			default:
				ent = append(ent, []byte(strconv.Quote(string(c)))...)
			}
		}

		enc = append(enc, ent...)
	}

	enc = append(enc, '}')
	if _, err := w.wr.Write(enc); err != nil {
		return err
	}

	w.rowsWritten++
	return nil
}

func (w *JsonWriter) writeArrayRow(row [][]byte) error {
	enc := []byte{',', '\n', '['}
	if w.rowsWritten == 0 {
		enc = enc[1:]
	}
	for i, c := range row {
		f := w.ds.Schema.Fields[i]
		ent := []byte(",")
		if i == 0 {
			ent = ent[1:]
		}
		if c == nil || len(c) == 0 {
			ent = append(ent, []byte("null")...)
		} else {
			switch f.Type {
			case datatypes.String:
				ent = append(ent, []byte(strconv.Quote(string(c)))...)
			case datatypes.Float, datatypes.Integer:
				// TODO - decide on weather or not to supply default values
				// if len(c) == 0 {
				// ent = append(ent, []byte("0")...)
				// } else {
				ent = append(ent, c...)
				// }
			case datatypes.Boolean:
				// TODO - coerce to true & false specifically
				// if len(c) == 0 {
				// ent = append(ent, []byte("false")...)
				// }
				ent = append(ent, c...)
			default:
				ent = append(ent, []byte(strconv.Quote(string(c)))...)
			}
		}

		enc = append(enc, ent...)
	}

	enc = append(enc, ']')
	if _, err := w.wr.Write(enc); err != nil {
		return err
	}

	w.rowsWritten++
	return nil
}

func (w *JsonWriter) Close() error {
	_, err := w.wr.Write([]byte{'\n', ']'})
	return err
}

type JsonReader struct {
}
