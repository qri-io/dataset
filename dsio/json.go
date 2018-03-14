package dsio

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
)

// JSONReader implements the RowReader interface for the JSON data format
type JSONReader struct {
	rowsRead    int
	initialized bool
	scanMode    scanMode // are we scanning an object or an array? default: array.
	st          *dataset.Structure
	sc          *bufio.Scanner
	objKey      string
}

// NewJSONReader creates a reader from a structure and read source
func NewJSONReader(st *dataset.Structure, r io.Reader) (*JSONReader, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for JSON reader")
		log.Debug(err.Error())
		return nil, err
	}

	sc := bufio.NewScanner(r)
	jr := &JSONReader{
		st: st,
		sc: sc,
	}
	sc.Split(jr.scanJSONEntry)
	// TODO - this is an interesting edge case. Need a big buffer for truly huge tokens.
	// let's create an issue to discuss. It might make sense to store the size of the largest
	// entry in the dataset as a structure definition
	sc.Buffer(nil, 256*1024)

	sm, err := schemaScanMode(st.Schema)
	jr.scanMode = sm
	return jr, err
}

// Structure gives this writer's structure
func (r *JSONReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one JSON record from the reader
func (r *JSONReader) ReadEntry() (Entry, error) {
	ent := Entry{}
	more := r.sc.Scan()
	if !more {
		return ent, fmt.Errorf("EOF")
	}
	r.rowsRead++

	if r.sc.Err() != nil {
		log.Debug(r.sc.Err())
		return ent, r.sc.Err()
	}

	if err := json.Unmarshal(r.sc.Bytes(), &ent.Value); err != nil {
		log.Debug(err.Error())
		return ent, err
	}

	if r.scanMode == smObject {
		ent.Key = r.objKey
	}

	return ent, nil
}

// initialIndex sets the scanner up to read data, advancing until the first
// entry in the top level array & setting the scanner split func to scan objects
func initialIndex(data []byte) (md scanMode, skip int, err error) {
	typ := JSONArrayOrObject(data)
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

// JSONArrayOrObject examines bytes checking if the outermost
// closure is an array or object
func JSONArrayOrObject(value []byte) string {
	for _, b := range value {
		switch b {
		case '"':
			return ""
		case '{':
			return "object"
		case '[':
			return "array"
		}
	}
	return ""
}

var moars = 0

// scanJSONEntry scans according to json value types (object, array, string, boolean, number, null, and integer)
func (r *JSONReader) scanJSONEntry(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
		return r.scanObjectEntry(data, atEOF)
	}

	return scanEntry(data, atEOF)
}

func (r *JSONReader) scanObjectEntry(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
	r.objKey = string(key)

	vadv, val, e := scanEntry(data[stradv:], atEOF)
	if val == nil || e != nil {
		return vadv, val, e
	}

	return stradv + vadv, val, e
}

func scanEntry(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
			err := fmt.Errorf("unexpected error scanning %s value", tok)
			log.Debug(err.Error())
			return 0, nil, err
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
		err := fmt.Errorf("schema required for JSON writer")
		log.Debug(err.Error())
		return nil, err
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

// WriteEntry writes one JSON record to the writer
func (w *JSONWriter) WriteEntry(ent Entry) error {
	defer func() {
		w.rowsWritten++
	}()
	if w.rowsWritten == 0 {
		open := []byte{'['}
		if w.scanMode == smObject {
			open = []byte{'{'}
		}
		if _, err := w.wr.Write(open); err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error writing initial `%s`: %s", string(open), err.Error())
		}
	}

	data, err := w.valBytes(ent)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	enc := []byte{','}
	if w.rowsWritten == 0 {
		enc = []byte{}
	}

	_, err = w.wr.Write(append(enc, data...))
	return err
}

func (w *JSONWriter) valBytes(ent Entry) ([]byte, error) {
	if w.scanMode == smArray {
		// TODO - add test that checks this is recording values & not entries
		return json.Marshal(ent.Value)
	}

	if ent.Key == "" {
		log.Debug("write empty key")
		return nil, fmt.Errorf("entry key cannot be empty")
	} else if w.keysWritten[ent.Key] == true {
		log.Debugf(`key already written: "%s"`, ent.Key)
		return nil, fmt.Errorf(`key already written: "%s"`, ent.Key)
	}
	w.keysWritten[ent.Key] = true

	data, err := json.Marshal(ent.Key)
	if err != nil {
		log.Debug(err.Error())
		return data, err
	}
	data = append(data, ':')
	val, err := json.Marshal(ent.Value)
	if err != nil {
		log.Debug(err.Error())
		return data, err
	}
	data = append(data, val...)
	return data, nil
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *JSONWriter) Close() error {
	// if WriteEntry is never called, write an empty array
	if w.rowsWritten == 0 {
		data := []byte("[]")
		if w.scanMode == smObject {
			data = []byte("{}")
		}

		if _, err := w.wr.Write(data); err != nil {
			log.Debug(err.Error())
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
		log.Debug(err.Error())
		return fmt.Errorf("error closing writer: %s", err.Error())
	}
	return nil
}
