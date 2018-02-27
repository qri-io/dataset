package dsio

// import (
// 	"bufio"
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"io"

// 	"github.com/qri-io/dataset"
// 	"github.com/qri-io/dataset/vals"
// 	"github.com/qri-io/jsonschema"
// 	"github.com/ugorji/go/codec"
// )

// // CBORReader implements the RowReader interface for the CBOR data format
// type CBORReader struct {
// 	rowsRead int
// }

// // NewCBORReader creates a reader from a structure and read source
// func NewCBORReader(st *dataset.Structure, r io.Reader) (*CBORReader, error) {
// 	rdr := codec.NewDecoder(r, &codec.CborHandle{
// 		TimeRFC3339: true,
// 	})
// 	return &CBORReader{}, nil
// }

// // Structure gives this writer's structure
// func (r *CBORReader) Structure() *dataset.Structure {
// 	return r.st
// }

// // ReadValue reads one CBOR record from the reader
// func (r *CBORReader) ReadValue() (vals.Value, error) {
// 	more := r.sc.Scan()
// 	if !more {
// 		return nil, fmt.Errorf("EOF")
// 	}
// 	r.rowsRead++

// 	if r.sc.Err() != nil {
// 		return nil, r.sc.Err()
// 	}

// 	val, err := vals.UnmarshalJSON(r.sc.Bytes())
// 	if err != nil {
// 		return nil, err
// 	}

// 	if r.scanMode == smObject {
// 		return vals.NewObjectValue(r.objKey, val), nil
// 	}

// 	return val, nil
// }

// // CBORWriter implements the RowWriter interface for
// // CBOR-formatted data
// type CBORWriter struct {
// 	rowsWritten int
// 	scanMode    scanMode
// 	st          *dataset.Structure
// 	wr          io.Writer
// 	keysWritten map[string]bool
// }

// // NewCBORWriter creates a Writer from a structure and write destination
// func NewCBORWriter(st *dataset.Structure, w io.Writer) (*CBORWriter, error) {
// 	if st.Schema == nil {
// 		return nil, fmt.Errorf("schema required for JSON writer")
// 	}

// 	jw := &CBORWriter{
// 		st: st,
// 		wr: w,
// 	}

// 	sm, err := schemaScanMode(st.Schema)
// 	jw.scanMode = sm
// 	if sm == smObject {
// 		jw.keysWritten = map[string]bool{}
// 	}

// 	return jw, err
// }

// // Structure gives this writer's structure
// func (w *CBORWriter) Structure() *dataset.Structure {
// 	return w.st
// }

// // ContainerType gives weather this writer is writing an array or an object
// func (w *CBORWriter) ContainerType() string {
// 	if w.scanMode == smObject {
// 		return "object"
// 	}
// 	return "array"
// }

// // WriteValue writes one JSON record to the writer
// func (w *CBORWriter) WriteValue(val vals.Value) error {
// 	defer func() {
// 		w.rowsWritten++
// 	}()
// 	if w.rowsWritten == 0 {
// 		open := []byte{'['}
// 		if w.scanMode == smObject {
// 			open = []byte{'{'}
// 		}
// 		if _, err := w.wr.Write(open); err != nil {
// 			return fmt.Errorf("error writing initial `%s`: %s", string(open), err.Error())
// 		}
// 	}

// 	data, err := w.valBytes(val)
// 	if err != nil {
// 		return err
// 	}

// 	enc := []byte{','}
// 	if w.rowsWritten == 0 {
// 		enc = []byte{}
// 	}

// 	_, err = w.wr.Write(append(enc, data...))
// 	return err
// }

// func (w *CBORWriter) valBytes(val vals.Value) ([]byte, error) {
// 	if w.scanMode == smArray {
// 		return json.Marshal(val)
// 	}

// 	if ov, ok := val.(vals.ObjectValue); ok {
// 		if w.keysWritten[ov.Key] == true {
// 			return nil, fmt.Errorf("key already written: \"%s\"", ov.Key)
// 		}
// 		w.keysWritten[ov.Key] = true

// 		data, err := json.Marshal(ov.Key)
// 		if err != nil {
// 			return data, err
// 		}
// 		data = append(data, ':')
// 		val, err := json.Marshal(ov.Value)
// 		if err != nil {
// 			return data, err
// 		}
// 		data = append(data, val...)
// 		return data, nil
// 	}

// 	return nil, fmt.Errorf("only vals.ObjectValue can be written to a JSON object writer")
// }

// // Close finalizes the writer, indicating no more records
// // will be written
// func (w *CBORWriter) Close() error {
// 	// // if WriteValue is never called, write an empty array
// 	// if w.rowsWritten == 0 {
// 	// 	data := []byte("[]")
// 	// 	if w.scanMode == smObject {
// 	// 		data = []byte("{}")
// 	// 	}

// 	// 	if _, err := w.wr.Write(data); err != nil {
// 	// 		return fmt.Errorf("error writing empty closure '%s': %s", string(data), err.Error())
// 	// 	}
// 	// 	return nil
// 	// }

// 	// cloze := []byte{']'}
// 	// if w.scanMode == smObject {
// 	// 	cloze = []byte{'}'}
// 	// }
// 	// _, err := w.wr.Write(cloze)
// 	// if err != nil {
// 	// 	return fmt.Errorf("error closing writer: %s", err.Error())
// 	// }
// 	return nil
// }
