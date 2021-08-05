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
	tlt         string
	st          *dataset.Structure
	objKey      string
	reader      *bufio.Reader
	close       func() error // close func from wrapped reader
	prevSize    int          // when buffer is extended, remember how much of the old buffer to discard
}

var _ EntryReader = (*JSONReader)(nil)

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

	tlt, err := GetTopLevelType(st)
	if err != nil {
		return nil, err
	}

	r, close, err := maybeWrapDecompressor(st, r)
	if err != nil {
		return nil, err
	}

	jr := &JSONReader{
		st:     st,
		reader: bufio.NewReaderSize(r, size),
		close:  close,
		tlt:    tlt,
	}
	return jr, nil
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
		if r.tlt == "object" {
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
	if r.tlt == "object" {
		if r.readTokenChar('}') {
			return ent, io.EOF
		}
	} else {
		if r.readTokenChar(']') {
			return ent, io.EOF
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
	if r.tlt == "object" {
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

// Close finalizes the reader
func (r *JSONReader) Close() error {
	if r.close != nil {
		return r.close()
	}
	return nil
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'
}

func (r *JSONReader) readTokenChar(ch byte) bool {
	buff := r.currentBuffer()
	if len(buff) > 0 && buff[0] == ch {
		// Either 0 or 1 characters are matched, only need to discard 1.
		_, _ = r.reader.Discard(1)
		return true
	}
	return false
}

func (r *JSONReader) readLiteralToken(tok []byte) bool {
	buff := r.currentBuffer()
	if len(tok) > len(buff) {
		// Buffer may contain a partial match, try reading ahead.
		var more bool
		buff, more = r.extendBuffer(buff)
		if !more {
			return false
		}
	}
	if len(tok) <= len(buff) && bytes.Compare(tok, buff[0:len(tok)]) == 0 {
		// If the buffer was extended, only discard the new bytes.
		_, _ = r.reader.Discard(len(tok) - r.prevSize)
		return true
	}
	return false
}

func (r *JSONReader) peekNextChar() byte {
	buff := r.currentBuffer()
	if len(buff) > 0 {
		return buff[0]
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
	r.prevSize = 0
	// Skip whitespace, returned buffer will start with non-whitepsace.
	skip := 0
	for {
		if skip >= len(buff) {
			var more bool
			buff, more = r.extendBuffer(buff)
			if !more {
				break
			}
		}
		if isWhitespace(buff[skip]) {
			skip++
		} else {
			break
		}
	}
	// Discard whitespace characters, move the buffer forward.
	if skip > 0 {
		_, _ = r.reader.Discard(skip - r.prevSize)
		r.prevSize = 0
		buff = buff[skip:]
	}
	return buff
}

func (r *JSONReader) extendBuffer(orig []byte) ([]byte, bool) {
	// Preserve the contents of the existing buffer.
	preserve := append([]byte(nil), orig...)
	// Keep track of buffer extension, to figure out how much to discard later.
	size := r.reader.Buffered()
	r.prevSize += size
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

func (r *JSONReader) extractFromBuffer(buffer []byte, i int) string {
	text := string(buffer[0:i])
	_, _ = r.reader.Discard(i - r.prevSize)
	r.prevSize = 0
	return text
}

func (r *JSONReader) extractBytesFromBuffer(buffer []byte, i int) []byte {
	d := buffer[0:i]
	_, _ = r.reader.Discard(i - r.prevSize)
	r.prevSize = 0
	return d
}

func (r *JSONReader) readString() (string, error) {
	buff := r.currentBuffer()
	i := 0
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
			// TODO(b5): this is slow, but necessary to properly handle complex JSON string
			// escape sequences. We should replace this with a hand-rolled JSON string parser
			// but add a benchmark first to confirm we're acutally engineering a performance
			// pickup
			var str string
			err := json.Unmarshal(r.extractBytesFromBuffer(buff, i), &str)
			return str, err
		}
		i++
	}
	return "", fmt.Errorf("Expected: closing '\"' for string")
}

func (r *JSONReader) readNumber() (interface{}, error) {
	buff := r.currentBuffer()
	isFloat := false
	i := 0
	for {
		if i >= len(buff) {
			var more bool
			buff, more = r.extendBuffer(buff)
			if !more {
				break
			}
		}
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
			return strconv.ParseFloat(r.extractFromBuffer(buff, i), 64)
		}
		num, err := strconv.Atoi(r.extractFromBuffer(buff, i))
		if err != nil {
			return nil, err
		}
		return int64(num), nil
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
	tlt         string
	indent      string
	st          *dataset.Structure
	wr          io.Writer
	close       func() error // close func from wrapped writer
	keysWritten map[string]bool
}

// NewJSONWriter creates a Writer from a structure and write destination
func NewJSONWriter(st *dataset.Structure, w io.Writer) (*JSONWriter, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for JSON writer")
		log.Debug(err.Error())
		return nil, err
	}

	tlt, err := GetTopLevelType(st)
	if err != nil {
		return nil, err
	}

	w, close, err := maybeWrapCompressor(st, w)
	if err != nil {
		return nil, err
	}

	jw := &JSONWriter{
		st:    st,
		wr:    w,
		close: close,
		tlt:   tlt,
	}

	if jw.tlt == "object" {
		jw.keysWritten = map[string]bool{}
	}
	return jw, nil
}

// NewJSONPrettyWriter creates a Writer that writes pretty indented JSON
func NewJSONPrettyWriter(st *dataset.Structure, w io.Writer, indent string) (*JSONWriter, error) {
	jw, err := NewJSONWriter(st, w)
	if err != nil {
		return nil, err
	}
	jw.indent = indent
	return jw, nil
}

// Structure gives this writer's structure
func (w *JSONWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteEntry writes one JSON record to the writer
func (w *JSONWriter) WriteEntry(ent Entry) error {
	defer func() {
		w.rowsWritten++
	}()
	if w.rowsWritten == 0 {
		open := []byte{'['}
		if w.tlt == "object" {
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

	// If between elems, put a comma. If pretty, newline as well.
	enc := []byte{','}
	if w.rowsWritten == 0 {
		enc = []byte{}
	}
	if w.indent != "" {
		enc = append(enc, []byte{'\n'}...)
	}

	_, err = w.wr.Write(append(enc, data...))
	return err
}

func (w *JSONWriter) valBytes(ent Entry) (data []byte, err error) {
	if w.tlt == "array" {
		// TODO - add test that checks this is recording values & not entries
		if w.indent != "" {
			data, err = json.MarshalIndent(ent.Value, w.indent, w.indent)
			data = append([]byte(w.indent), data...)
			return data, err
		}
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

	// Write the key
	if w.indent != "" {
		data, err = json.MarshalIndent(ent.Key, w.indent, w.indent)
		data = append([]byte(w.indent), data...)
	} else {
		data, err = json.Marshal(ent.Key)
	}
	if err != nil {
		log.Debug(err.Error())
		return data, err
	}

	// Write the value
	data = append(data, ':')
	var val []byte
	if w.indent != "" {
		val, err = json.MarshalIndent(ent.Value, w.indent, w.indent)
		val = append([]byte(w.indent), val...)
	} else {
		val, err = json.Marshal(ent.Value)
	}
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
	// if WriteEntry is never called, write an empty top level type
	if w.rowsWritten == 0 {
		data := []byte("[]")
		if w.tlt == "object" {
			data = []byte("{}")
		}

		if _, err := w.wr.Write(data); err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error writing empty closure '%s': %s", string(data), err.Error())
		}
		if w.close != nil {
			return w.close()
		}
		return nil
	}

	// If pretty, newline
	if w.indent != "" {
		w.wr.Write([]byte{'\n'})
	}

	cloze := []byte{']'}
	if w.tlt == "object" {
		cloze = []byte{'}'}
	}
	_, err := w.wr.Write(cloze)
	if err != nil {
		log.Debug(err.Error())
		return fmt.Errorf("error closing writer: %s", err.Error())
	}
	if w.close != nil {
		return w.close()
	}
	return nil
}
