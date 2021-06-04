package dsio

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
)

// NDJSONReader implements the EntryReader interface for the JSON data format
type NDJSONReader struct {
	entriesRead int
	st          *dataset.Structure
	scanner     *bufio.Scanner
	close       func() error // close func from wrapped reader
	prevSize    int          // when buffer is extended, remember how much of the old buffer to discard
}

var _ EntryReader = (*NDJSONReader)(nil)

// NewNDJSONReader creates a reader from a structure and read source
func NewNDJSONReader(st *dataset.Structure, r io.Reader) (*NDJSONReader, error) {
	// Huge buffer 5MiB, b/c NDJSON lines can be very long
	return NewNDJSONReaderSize(st, r, 5*1000000)
}

// NewNDJSONReaderSize creates a reader from a structure, read source, and buffer size
func NewNDJSONReaderSize(st *dataset.Structure, r io.Reader, size int) (*NDJSONReader, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for NDJSON reader")
		log.Debug(err.Error())
		return nil, err
	}

	tlt, err := GetTopLevelType(st)
	if err != nil {
		return nil, err
	}
	if tlt != "array" {
		return nil, fmt.Errorf("NDJSON top level type must be 'array'")
	}

	r, close, err := maybeWrapDecompressor(st, r)
	if err != nil {
		return nil, err
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1000, size), size)

	ndjr := &NDJSONReader{
		st:      st,
		scanner: sc,
		close:   close,
	}
	return ndjr, nil
}

// Structure gives this writer's structure
func (r *NDJSONReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one JSON record from the reader
func (r *NDJSONReader) ReadEntry() (Entry, error) {
	if more := r.scanner.Scan(); !more {
		if err := r.scanner.Err(); err != nil {
			return Entry{}, err
		}
		return Entry{}, io.EOF
	}

	var v interface{}
	if err := json.Unmarshal(r.scanner.Bytes(), &v); err != nil {
		return Entry{}, err
	}

	ent := Entry{
		Index: r.entriesRead,
		Value: v,
	}

	r.entriesRead++
	return ent, nil
}

// Close finalizes the reader
func (r *NDJSONReader) Close() error {
	if r.close != nil {
		return r.close()
	}
	return nil
}

// NDJSONWriter implements the EntryWriter interface for
// Newline-Deliminted-JSON-formatted data
type NDJSONWriter struct {
	rowsWritten int
	st          *dataset.Structure
	wr          io.Writer
	enc         *json.Encoder
	close       func() error // close func from wrapped writer
}

var _ EntryWriter = (*NDJSONWriter)(nil)

// NewNDJSONWriter creates a Writer from a structure and write destination
func NewNDJSONWriter(st *dataset.Structure, w io.Writer) (*NDJSONWriter, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for NDJSON writer")
		log.Debug(err.Error())
		return nil, err
	}

	w, close, err := maybeWrapCompressor(st, w)
	if err != nil {
		return nil, err
	}

	jw := &NDJSONWriter{
		st:    st,
		wr:    w,
		enc:   json.NewEncoder(w),
		close: close,
	}

	return jw, nil
}

// Structure gives this writer's structure
func (w *NDJSONWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteEntry writes one JSON entry to the writer
func (w *NDJSONWriter) WriteEntry(ent Entry) error {
	return w.enc.Encode(ent.Value)
}

// Close finalizes the writer
func (w *NDJSONWriter) Close() error {
	if w.close != nil {
		return w.close()
	}
	return nil
}
