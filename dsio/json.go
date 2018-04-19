package dsio

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/qri-io/dataset"
)

// JSONReader implements the RowReader interface for the JSON data format
type JSONReader struct {
	entriesRead int
	initialized bool
	scanMode    scanMode // are we scanning an object or an array? default: array.
	st          *dataset.Structure
	objKey      string
	reader      *bufio.Reader
	begin       int
}

// NewJSONReader creates a reader from a structure and read source
func NewJSONReader(st *dataset.Structure, r io.Reader) (*JSONReader, error) {
	// Huge buffer (a quarter of a MB) to speed up string reads.
	return NewJSONReaderSize(st, r, 256*1024)
}

// NewJSONReaderSize creates a reader from a structure, read source, and buffer size
func NewJSONReaderSize(st *dataset.Structure, r io.Reader, size int) (*JSONReader, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for JSON reader")
		log.Debug(err.Error())
		return nil, err
	}

	reader := bufio.NewReaderSize(r, size)
	jr := &JSONReader{
		st:     st,
		reader: reader,
	}
	sm, err := schemaScanMode(st.Schema)
	jr.scanMode = sm
	return jr, err
}

// Structure gives this writer's structure
func (r *JSONReader) Structure() *dataset.Structure {
	return r.st
}

const blockSize = 4096

// ReadEntry reads one JSON record from the reader
func (r *JSONReader) ReadEntry() (Entry, error) {
	ent := Entry{}

	// Fill up buffer.
	_, _ = r.reader.Peek(blockSize)

	// Open JSON container the first time this is called.
	if !r.initialized {
		if r.scanMode == smObject {
			if !r.readTokenChar('{') {
				return ent, fmt.Errorf("Expected: opening object '{'")
			}
		} else {
			if !r.readTokenChar('[') {
				return ent, fmt.Errorf("Expected: opening array '['")
			}
		}
	}

	// Close JSON container if it is complete, signaling EOF.
	if r.scanMode == smObject {
		if r.readTokenChar('}') {
			return ent, fmt.Errorf("EOF")
		}
	} else {
		if r.readTokenChar(']') {
			return ent, fmt.Errorf("EOF")
		}
	}

	// Need a separator between elements, but not before the very first.
	if r.initialized {
		if !r.readTokenChar(',') {
			return ent, fmt.Errorf("Expected: separator ','")
		}
	}
	r.initialized = true

	// Read actual entry, format depends depends upon mode.
	if r.scanMode == smObject {
		key, val, err := r.readKeyValuePair()
		ent.Key = key
		ent.Value = val
		if err != nil {
			return ent, err
		}
	} else {
		val, err := r.readValue()
		ent.Index = r.entriesRead
		ent.Value = val
		if err != nil {
			return ent, err
		}
	}
	r.entriesRead++
	return ent, nil
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'
}

func (r *JSONReader) readTokenChar(ch byte) bool {
	buff := r.currentBuffer()
	i := 0
	for i < len(buff) && isWhitespace(buff[i]) {
		i++
	}
	if i < len(buff) && buff[i] == ch {
		i++
		_, _ = r.reader.Discard(i)
		return true
	}
	return false
}

func (r *JSONReader) readLiteralToken(tok []byte) bool {
	buff := r.currentBuffer()
	i := 0
	for i < len(buff) && isWhitespace(buff[i]) {
		i++
	}
	if i+len(tok) < len(buff) && bytes.Compare(tok, buff[i:i+len(tok)]) == 0 {
		i += len(tok)
		_, _ = r.reader.Discard(i)
		return true
	}
	return false
}

func (r *JSONReader) peekNextChar() byte {
	buff := r.currentBuffer()
	i := 0
	for i < len(buff) && isWhitespace(buff[i]) {
		i++
	}
	if i < len(buff) {
		_, _ = r.reader.Discard(i)
		return buff[i]
	}
	return 0
}

func (r *JSONReader) readValue() (interface{}, error) {
	b := r.peekNextChar()
	switch b {
	case 'n':
		if r.readLiteralToken([]byte("null")) {
			return nil, nil
		}
		return nil, fmt.Errorf("Expected: null")
	case 't':
		if r.readLiteralToken([]byte("true")) {
			return true, nil
		}
		return nil, fmt.Errorf("Expected: true")
	case 'f':
		if r.readLiteralToken([]byte("false")) {
			return false, nil
		}
		return nil, fmt.Errorf("Expected: false")
	case '"':
		return r.readString()
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return r.readNumber()
	case '{':
		return r.readObject()
	case '[':
		return r.readArray()
	default:
		return nil, nil
	}
}

func (r *JSONReader) currentBuffer() []byte {
	buff, _ := r.reader.Peek(r.reader.Buffered())
	r.begin = 0
	return buff
}

func (r *JSONReader) extendBuffer(orig []byte) ([]byte, bool) {
	// Preserve the contents of the existing buffer.
	preserve := append([]byte(nil), orig...)
	// Keep track of buffer extension, to figure out how much to discard later.
	size := r.reader.Buffered()
	r.begin += size
	// Clear the reader's buffer, fill it back up.
	_, _ = r.reader.Discard(size)
	_, _ = r.reader.Peek(blockSize)
	size = r.reader.Buffered()
	if size > 0 {
		// If successful, append buffers.
		extend, _ := r.reader.Peek(size)
		return append(preserve, extend...), true
	}
	return orig, false
}

func (r *JSONReader) readString() (string, error) {
	buff := r.currentBuffer()
	s := 0
	for s < len(buff) && isWhitespace(buff[s]) {
		s++
	}
	i := s
	if i < len(buff) && buff[i] == '"' {
		i++
	} else {
		return "", fmt.Errorf("Expected: string")
	}

	for {
		if i >= len(buff) {
			var more bool
			buff, more = r.extendBuffer(buff)
			if !more {
				break
			}
		}
		if buff[i] == '\\' {
			i++
		} else if buff[i] == '"' {
			i++
			_, _ = r.reader.Discard(i - r.begin)
			return strconv.Unquote(string(buff[s:i]))
		}
		i++
	}
	return "", fmt.Errorf("Expected: closing '\"' for string")
}

func (r *JSONReader) readNumber() (interface{}, error) {
	buff := r.currentBuffer()
	isFloat := false
	i := 0
	for i < len(buff) {
		if buff[i] >= '0' && buff[i] <= '9' {
			i++
		} else if buff[i] == '.' || buff[i] == 'e' || buff[i] == 'E' || buff[i] == '+' {
			isFloat = true
			i++
		} else if buff[i] == '-' {
			i++
		} else {
			break
		}
	}
	if i > 0 {
		if isFloat {
			_, _ = r.reader.Discard(i)
			return strconv.ParseFloat(string(buff[0:i]), 64)
		}
		_, _ = r.reader.Discard(i)
		return strconv.Atoi(string(buff[0:i]))
	}
	return 0, fmt.Errorf("Expected: number")
}

func (r *JSONReader) readObject() (interface{}, error) {
	if !r.readTokenChar('{') {
		return nil, fmt.Errorf("Expected: opening '{' for object")
	}
	obj := make(map[string]interface{})
	if r.readTokenChar('}') {
		return obj, nil
	}
	// Read first key, value pair
	key, val, err := r.readKeyValuePair()
	if err != nil {
		return nil, err
	}
	obj[key] = val
	// Read other key, value pairs
	for {
		// ensure a sufficent amount of data is buffered
		if r.reader.Buffered() < r.reader.Size() {
			r.reader.Peek(r.reader.Size())
		}

		if r.readTokenChar('}') {
			break
		} else if !r.readTokenChar(',') {
			return nil, fmt.Errorf("Expected: ',' to separate elements")
		}
		key, val, err := r.readKeyValuePair()
		if err != nil {
			return obj, err
		}
		obj[key] = val
	}
	return obj, nil
}

func (r *JSONReader) readArray() ([]interface{}, error) {
	if !r.readTokenChar('[') {
		return nil, fmt.Errorf("Expected: opening '[' for array")
	}
	array := make([]interface{}, 0)
	if r.readTokenChar(']') {
		return array, nil
	}
	// Read first element.
	val, err := r.readValue()
	if err != nil {
		return array, nil
	}
	array = append(array, val)
	// Read the rest of the elements.
	for {
		// ensure a sufficent amount of data is buffered
		if r.reader.Buffered() < r.reader.Size() {
			r.reader.Peek(r.reader.Size())
		}

		if r.readTokenChar(']') {
			break
		} else if !r.readTokenChar(',') {
			buff := r.currentBuffer()
			log.Error(string(buff))
			return nil, fmt.Errorf("Expected: ',' to separate elements")
		}
		val, err := r.readValue()
		if err != nil {
			return array, err
		}
		array = append(array, val)
	}
	return array, nil
}

func (r *JSONReader) readKeyValuePair() (string, interface{}, error) {
	key, err := r.readString()
	if err != nil {
		return "", nil, err
	}
	if !r.readTokenChar(':') {
		return "", nil, fmt.Errorf("Expected: ':' to separate key and value")
	}
	val, err := r.readValue()
	if err != nil {
		return "", nil, err
	}
	return key, val, nil
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
