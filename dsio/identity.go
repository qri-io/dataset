package dsio

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset"
)

// NewIdentityReader creates an EntryReader from native go types, passed in
// data must be of type []interface{} or map[string]interface{}
func NewIdentityReader(st *dataset.Structure, data interface{}) (*IdentityReader, error) {
	r := &IdentityReader{st: st}

	if md, ok := data.(map[string]interface{}); ok {
		r.entries = r.iterateMap(md)
	} else if sd, ok := data.([]interface{}); ok {
		r.entries = r.iterateSlice(sd)
	} else {
		return nil, fmt.Errorf("cannot create entry reader from type %T", data)
	}

	return r, nil
}

// IdentityReader is a dsio.EntryReader that works with native go types
type IdentityReader struct {
	st      *dataset.Structure
	done    bool
	entries chan Entry
}

var _ EntryReader = (*IdentityReader)(nil)

// Structure gives the structure being read
func (r *IdentityReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one row of structured data from the reader
func (r *IdentityReader) ReadEntry() (Entry, error) {
	if r.done {
		return Entry{}, io.EOF
	}

	return <-r.entries, nil
}

// Close finalizes the reader
func (r *IdentityReader) Close() error {
	if !r.done {
		// drain channel to prevent leaking goroutine
		for !r.done {
			<-r.entries
		}
	}
	return nil
}

func (r *IdentityReader) iterateMap(data map[string]interface{}) chan Entry {
	res := make(chan Entry)

	go func() {
		for key, val := range data {
			res <- Entry{Key: key, Value: val}
		}
		r.done = true
	}()

	return res
}

func (r *IdentityReader) iterateSlice(data []interface{}) chan Entry {
	res := make(chan Entry)

	go func() {
		for i, val := range data {
			res <- Entry{Index: i, Value: val}
		}
		r.done = true
	}()

	return res
}

// IdentityWriter is a dsio.EntryWriter that works with native go types
type IdentityWriter struct {
	st *dataset.Structure
}

// Structure gives the structure being written
func (w *IdentityWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteEntry writes one "row" of structured data to the Writer
func (w *IdentityWriter) WriteEntry(e Entry) error {
	return nil
}

// Close finalizes the writer, indicating all entries
// have been written
func (w *IdentityWriter) Close() error {
	return nil
}
