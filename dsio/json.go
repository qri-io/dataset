package dsio

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
)

// JSONReader implements the RowReader interface for the JSON data format
type JSONReader struct {
	rowsRead    int
	initialized bool
	st          *dataset.Structure
	sc          *bufio.Scanner
}

// NewJSONReader creates a reader from a structure and read source
func NewJSONReader(st *dataset.Structure, r io.Reader) *JSONReader {
	sc := bufio.NewScanner(r)
	jr := &JSONReader{
		st: st,
		sc: sc,
	}
	sc.Split(jr.scanJSONRow)
	return jr
}

// Structure gives this writer's structure
func (r *JSONReader) Structure() *dataset.Structure {
	return r.st
}

// ReadValue reads one JSON record from the reader
func (r *JSONReader) ReadValue() (vals.Value, error) {
	more := r.sc.Scan()
	if !more {
		return nil, fmt.Errorf("EOF")
	}
	r.rowsRead++

	if r.sc.Err() != nil {
		return nil, r.sc.Err()
	}

	return vals.UnmarshalJSON(r.sc.Bytes())
}

// initialIndex sets the scanner up to read data, advancing until the first
// entry in the top level array & setting the scanner split func to scan objects
func initialIndex(data []byte) (skip int, err error) {
	typ := vals.JSONArrayOrObject(data)
	if typ == "" {
		// might not have initial closure, request more data
		return -1, err
	}
	if typ == "object" {
		return 0, fmt.Errorf("jsonReader top level must be an array")
	}

	// grab first opening bracked index to advance past
	// initial array closure
	idx := bytes.IndexByte(data, '[')
	return idx + 1, nil
}

// scanJSONRow scans according to json closures ([] and {})
func (r *JSONReader) scanJSONRow(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	depth := 0
	starti := -1
	stopi := -1

	if !r.initialized {
		skip, err := initialIndex(data)
		if err != nil {
			return 0, nil, err
		}
		if skip > 0 {
			r.initialized = true
			data = data[skip:]
		}
	}

LOOP:
	for i, b := range data {
		switch b {
		// case '"':
		// 	if depth == 0 {
		// 		starti = i
		// 		depth++
		// 	} else if depth > 0 {
		// 		depth--
		// 		if depth == 0 {
		// 			stopi = i + 1
		// 			break LOOP
		// 		}
		// 	} else {
		// 		return len(data), nil, nil
		// 	}
		// case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':

		case '{', '[':
			if depth == 0 {
				starti = i
			}
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				stopi = i + 1
				break LOOP
			} else if depth < 0 {
				// if we encounter a closing bracket
				// before any depth, it's the end of the file
				return len(data), nil, nil
			}
		}
	}

	if stopi == -1 || starti == -1 {
		return 0, nil, nil
	}

	// return sliced data
	if starti < stopi {
		// TODO - this should semantically advance past a comma or something...
		return stopi + 1, data[starti:stopi], nil
	}

	// Request more data.
	return 0, nil, nil
}

// JSONWriter implements the RowWriter interface for
// JSON-formatted data
type JSONWriter struct {
	writeObjects bool
	rowsWritten  int
	st           *dataset.Structure
	wr           io.Writer
}

// NewJSONWriter creates a Writer from a structure and write destination
func NewJSONWriter(st *dataset.Structure, w io.Writer) *JSONWriter {
	writeObjects := true
	if opt, ok := st.FormatConfig.(*dataset.JSONOptions); ok {
		writeObjects = !opt.ArrayEntries
	}
	return &JSONWriter{
		writeObjects: writeObjects,
		st:           st,
		wr:           w,
	}
}

// Structure gives this writer's structure
func (w *JSONWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteValue writes one JSON record to the writer
func (w *JSONWriter) WriteValue(val vals.Value) error {
	defer func() {
		w.rowsWritten++
	}()
	if w.rowsWritten == 0 {
		if _, err := w.wr.Write([]byte{'['}); err != nil {
			return fmt.Errorf("error writing initial `[`: %s", err.Error())
		}
	}

	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	enc := []byte{',', '\n'}
	if w.rowsWritten == 0 {
		enc = enc[1:]
	}

	// if w.writeObjects {
	// 	return w.writeObjectRow(val)
	// }
	// return w.writeArrayRow(val)
	_, err = w.wr.Write(append(enc, data...))
	return err
}

func (w *JSONWriter) writeObjectRow(row [][]byte) error {
	enc := []byte{',', '\n', '{'}
	if w.rowsWritten == 0 {
		enc = enc[1:]
	}
	// TODO - restore
	// for i, c := range row {
	// 	f := w.st.Schema.Fields[i]
	// 	ent := []byte(",\"" + f.Name + "\":")
	// 	if i == 0 {
	// 		ent = ent[1:]
	// 	}
	// 	if c == nil || len(c) == 0 {
	// 		ent = append(ent, []byte("null")...)
	// 	} else {
	// 		switch f.Type {
	// 		case vals.String:
	// 			ent = append(ent, []byte(strconv.Quote(string(c)))...)
	// 		case vals.Float, vals.Integer:
	// 			// if len(c) == 0 {
	// 			// 	ent = append(ent, []byte("null")...)
	// 			// } else {
	// 			// 	ent = append(ent, c...)
	// 			// }
	// 			ent = append(ent, c...)
	// 		case vals.Boolean:
	// 			// TODO - coerce to true & false specifically
	// 			ent = append(ent, c...)
	// 		case vals.JSON:
	// 			ent = append(ent, c...)
	// 		default:
	// 			ent = append(ent, []byte(strconv.Quote(string(c)))...)
	// 		}
	// 	}

	// 	enc = append(enc, ent...)
	// }

	enc = append(enc, '}')
	if _, err := w.wr.Write(enc); err != nil {
		return fmt.Errorf("error writing json object row to writer: %s", err.Error())
	}

	w.rowsWritten++
	return nil
}

func (w *JSONWriter) writeArrayRow(row [][]byte) error {
	enc := []byte{',', '\n', '['}
	if w.rowsWritten == 0 {
		enc = enc[1:]
	}
	// TODO - restore
	// for i, c := range row {
	// 	f := w.st.Schema.Fields[i]
	// 	ent := []byte(",")
	// 	if i == 0 {
	// 		ent = ent[1:]
	// 	}
	// 	if c == nil || len(c) == 0 {
	// 		ent = append(ent, []byte("null")...)
	// 	} else {
	// 		switch f.Type {
	// 		case vals.String:
	// 			ent = append(ent, []byte(strconv.Quote(string(c)))...)
	// 		case vals.Float, vals.Integer:
	// 			// TODO - decide on weather or not to supply default values
	// 			// if len(c) == 0 {
	// 			// ent = append(ent, []byte("0")...)
	// 			// } else {
	// 			ent = append(ent, c...)
	// 			// }
	// 		case vals.Boolean:
	// 			// TODO - coerce to true & false specifically
	// 			// if len(c) == 0 {
	// 			// ent = append(ent, []byte("false")...)
	// 			// }
	// 			ent = append(ent, c...)
	// 		case vals.JSON:
	// 			ent = append(ent, c...)
	// 		default:
	// 			ent = append(ent, []byte(strconv.Quote(string(c)))...)
	// 		}
	// 	}

	// 	enc = append(enc, ent...)
	// }

	enc = append(enc, ']')
	if _, err := w.wr.Write(enc); err != nil {
		return fmt.Errorf("error writing closing `]`: %s", err.Error())
	}

	w.rowsWritten++
	return nil
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *JSONWriter) Close() error {
	// if WriteValue is never called, write an empty array
	if w.rowsWritten == 0 {
		if _, err := w.wr.Write([]byte("[]")); err != nil {
			return fmt.Errorf("error writing initial `[`: %s", err.Error())
		}
		return nil
	}

	_, err := w.wr.Write([]byte{'\n', ']'})
	if err != nil {
		return fmt.Errorf("error closing writer: %s", err.Error())
	}
	return nil
}
