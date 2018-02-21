package dsio

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/qri-io/jsonschema"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
)

// JSONReader implements the RowReader interface for the JSON data format
type JSONReader struct {
	rowsRead    int
	initialized bool
	scanMode    scanMode // are we scanning an object or an array? default: array.
	st          *dataset.Structure
	sc          *bufio.Scanner
}

func schemaScanMode(sc *jsonschema.RootSchema) (scanMode, error) {
	if vt, ok := sc.Validators["type"]; ok {
		// TODO - lol go PR jsonschema to export access to this instead of this
		// silly validation hack
		if errs := vt.Validate(map[string]interface{}{}); len(errs) == 0 {
			return smObject, nil
		} else if errs := vt.Validate([]interface{}{}); len(errs) == 0 {
			return smArray, nil
		}
	}
	return smArray, fmt.Errorf("invalid schema for JSON data format. root must be either an array or object type")
}

type scanMode int

const (
	smArray scanMode = iota
	smObject
)

// NewJSONReader creates a reader from a structure and read source
func NewJSONReader(st *dataset.Structure, r io.Reader) (*JSONReader, error) {
	if st.Schema == nil {
		return nil, fmt.Errorf("schema required for JSON reader")
	}

	sc := bufio.NewScanner(r)
	jr := &JSONReader{
		st: st,
		sc: sc,
	}
	sc.Split(jr.scanJSONValue)

	sm, err := schemaScanMode(st.Schema)
	jr.scanMode = sm
	return jr, err
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
func initialIndex(data []byte) (md scanMode, skip int, err error) {
	typ := vals.JSONArrayOrObject(data)
	if typ == "" {
		// might not have initial closure, request more data
		return smArray, -1, err
	}

	if typ == "object" {
		// grab first opening curly brace index to advance past
		// initial object closure
		idx := bytes.IndexByte(data, '{')
		return smObject, idx + 1, nil
	}

	// grab first opening bracket index to advance past
	// initial array closure
	idx := bytes.IndexByte(data, '[')
	return smArray, idx + 1, nil
}

var moars = 0

// scanJSONValue scans according to json closures ([] and {})
func (r *JSONReader) scanJSONValue(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if !r.initialized {
		sm, skip, err := initialIndex(data)
		if err != nil {
			return 0, nil, err
		}
		if skip > 0 {
			r.scanMode = sm
			r.initialized = true
			data = data[skip:]
		}
		return skip, nil, nil
	}

	if r.scanMode == smObject {
		return scanObjectValue(data, atEOF)
	}

	return scanValue(data, atEOF)
}

func scanObjectValue(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for _, b := range data {
		if b == ':' {
			break
		} else if b == '}' {
			if atEOF {
				return len(data), nil, nil
			}
			return 0, nil, nil
		}
	}

	// scan key
	stradv, key, e := scanString(data, atEOF)
	if key == nil || e != nil {
		return stradv, key, e
	}

	vadv, val, e := scanValue(data[stradv:], atEOF)
	if val == nil || e != nil {
		return vadv, val, e
	}

	return stradv + vadv, val, e
}

func scanValue(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for _, b := range data {
		switch b {
		case '"':
			return scanString(data, atEOF)
		case 'n':
			return scanNull(data, atEOF)
		case 't':
			return scanTrue(data, atEOF)
		case 'f':
			return scanFalse(data, atEOF)
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'e':
			return scanNumber(data, atEOF)
		case '{':
			return scanObject(data, atEOF)
		case '[':
			return scanArray(data, atEOF)
		case '}', ']':
			// if we encounter a closing bracket
			// before any depth, it's the end of the closure
			return len(data), nil, nil
		}
	}

	// Request more data.
	return 0, nil, nil
}

func strTokScanner(tok string) func([]byte, bool) (int, []byte, error) {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		start := bytes.Index(data, []byte(tok))
		if start == -1 {
			return 0, nil, fmt.Errorf("unexpected error scanning %s value", tok)
		}
		stop := start + len(tok)

		return advSep(stop, data), data[start:stop], nil
	}
}

var (
	scanNull  = strTokScanner("null")
	scanTrue  = strTokScanner("true")
	scanFalse = strTokScanner("false")
)

func scanNumber(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := -1
	stop := -1

LOOP:
	for i, b := range data {
		switch b {
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'e':
			if start == -1 {
				start = i
			}
		default:
			if start != -1 {
				stop = i
				break LOOP
			}
		}
	}

	if stop == -1 || start == -1 {
		return 0, nil, nil
	}

	return advSep(stop, data), data[start:stop], nil
}

func scanString(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := -1
	stop := -1

LOOP:
	for i, b := range data {
		switch b {
		case '"':

			if start == -1 {
				start = i
			} else {
				// skip escaped quote characters
				if data[i-1] == '\\' {
					break
				}

				stop = i + 1
				break LOOP
			}
		}
	}

	if stop == -1 || start == -1 {
		return 0, nil, nil
	}

	return advSep(stop, data), data[start:stop], nil
}

func scanObject(data []byte, atEOF bool) (advance int, token []byte, err error) {
	starti, stopi, depth := -1, -1, 0
	instring := false

LOOP:
	for i, b := range data {
		switch b {
		case '"':
			// skip escaped quote characters
			if instring && data[i-1] == '\\' {
				break
			}
			instring = !instring
		case '{':
			if !instring {
				if depth == 0 {
					starti = i
				}
				depth++
			}
		case '}':
			if !instring {
				depth--
				if depth == 0 {
					stopi = i + 1
					break LOOP
				}
			}
		}
	}

	if stopi == -1 || starti == -1 {
		return 0, nil, nil
	}

	// return sliced data
	if starti < stopi {
		return advSep(stopi, data), data[starti:stopi], nil
	}
	return 0, nil, nil
}

func scanArray(data []byte, atEOF bool) (advance int, token []byte, err error) {
	starti, stopi, depth := -1, -1, 0
	instring := false

LOOP:
	for i, b := range data {
		switch b {
		case '"':
			// skip escaped quote chars
			if instring && data[i-1] == '\\' {
				break
			}
			instring = !instring
		case '[':
			if !instring {
				if depth == 0 {
					starti = i
				}
				depth++
			}
		case ']':
			if !instring {
				depth--
				if depth == 0 {
					stopi = i + 1
					break LOOP
				}
			}
		}
	}
	if stopi == -1 || starti == -1 {
		return 0, nil, nil
	}
	// return sliced data
	if starti < stopi {
		return advSep(stopi, data), data[starti:stopi], nil
	}
	return 0, nil, nil
}

func advSep(start int, data []byte) int {
	if start > 0 {
		for i := start; i < len(data); i++ {
			if data[i] == ',' || data[i] == ':' {
				return i + 1
			}
		}
	}
	return start
}

// JSONWriter implements the RowWriter interface for
// JSON-formatted data
type JSONWriter struct {
	rowsWritten int
	scanMode    scanMode
	st          *dataset.Structure
	wr          io.Writer
	keysWritten map[string]bool
}

// NewJSONWriter creates a Writer from a structure and write destination
func NewJSONWriter(st *dataset.Structure, w io.Writer) (*JSONWriter, error) {
	if st.Schema == nil {
		return nil, fmt.Errorf("schema required for JSON writer")
	}

	jw := &JSONWriter{
		st: st,
		wr: w,
	}

	sm, err := schemaScanMode(st.Schema)
	jw.scanMode = sm
	if sm == smObject {
		jw.keysWritten = map[string]bool{}
	}

	return jw, err
}

// Structure gives this writer's structure
func (w *JSONWriter) Structure() *dataset.Structure {
	return w.st
}

// ContainerType gives weather this writer is writing an array or an object
func (w *JSONWriter) ContainerType() string {
	if w.scanMode == smObject {
		return "object"
	}
	return "array"
}

// WriteValue writes one JSON record to the writer
func (w *JSONWriter) WriteValue(val vals.Value) error {
	defer func() {
		w.rowsWritten++
	}()
	if w.rowsWritten == 0 {
		open := []byte{'['}
		if w.scanMode == smObject {
			open = []byte{'{'}
		}
		if _, err := w.wr.Write(open); err != nil {
			return fmt.Errorf("error writing initial `%s`: %s", string(open), err.Error())
		}
	}

	data, err := w.valBytes(val)
	if err != nil {
		return err
	}

	enc := []byte{','}
	if w.rowsWritten == 0 {
		enc = []byte{}
	}

	_, err = w.wr.Write(append(enc, data...))
	return err
}

func (w *JSONWriter) valBytes(val vals.Value) ([]byte, error) {
	if w.scanMode == smArray {
		return json.Marshal(val)
	}

	if ov, ok := val.(vals.ObjectValue); ok {
		if w.keysWritten[ov.Key] == true {
			return nil, fmt.Errorf("key already written: \"%s\"", ov.Key)
		}
		w.keysWritten[ov.Key] = true

		data, err := json.Marshal(ov.Key)
		if err != nil {
			return data, err
		}
		data = append(data, ':')
		val, err := json.Marshal(ov.Value)
		if err != nil {
			return data, err
		}
		data = append(data, val...)
		return data, nil
	}

	return nil, fmt.Errorf("only vals.ObjectValue can be written to a JSON object writer")
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *JSONWriter) Close() error {
	// if WriteValue is never called, write an empty array
	if w.rowsWritten == 0 {
		data := []byte("[]")
		if w.scanMode == smObject {
			data = []byte("{}")
		}

		if _, err := w.wr.Write(data); err != nil {
			return fmt.Errorf("error writing empty closure '%s': %s", string(data), err.Error())
		}
		return nil
	}

	cloze := []byte{']'}
	if w.scanMode == smObject {
		cloze = []byte{'}'}
	}
	_, err := w.wr.Write(cloze)
	if err != nil {
		return fmt.Errorf("error closing writer: %s", err.Error())
	}
	return nil
}
