package dsio

import (
	"bytes"

	"github.com/qri-io/dataset"
)

// EntryBuffer mimics the behaviour of bytes.Buffer, but with structured Dataa
// Read and Write are replaced with ReadRow and WriteEntry. It's worth noting
// that different data formats have idisyncrcies that affect the behavior
// of buffers and their output. For example, EntryBuffer won't write things like
// CSV header rows or enclosing JSON arrays until after the writer's
// Close method has been called.
type EntryBuffer struct {
	structure *dataset.Structure
	r         EntryReader
	w         EntryWriter
	buf       *bytes.Buffer
}

// NewEntryBuffer allocates a buffer, buffers should always be created with
// NewEntryBuffer, which will error if the provided structure is invalid for
// reading / writing
func NewEntryBuffer(st *dataset.Structure) (*EntryBuffer, error) {
	buf := &bytes.Buffer{}
	r, err := NewEntryReader(st, buf)
	if err != nil {
		return nil, err
	}
	w, err := NewEntryWriter(st, buf)
	if err != nil {
		return nil, err
	}

	return &EntryBuffer{
		structure: st,
		r:         r,
		w:         w,
		buf:       buf,
	}, nil
}

// Structure gives the underlying structure this buffer is using
func (b *EntryBuffer) Structure() *dataset.Structure {
	return b.structure
}

// ReadRow reads one row from the buffer
func (b *EntryBuffer) ReadRow() (Entry, error) {
	return b.r.ReadEntry()
}

// WriteEntry writes one row to the buffer
func (b *EntryBuffer) WriteEntry(e Entry) error {
	return b.w.WriteEntry(e)
}

// Close closes the writer portion of the buffer, which will affect
// underlying contents.
func (b *EntryBuffer) Close() error {
	return b.w.Close()
}

// Bytes gives the raw contents of the underlying buffer
func (b *EntryBuffer) Bytes() []byte {
	return b.buf.Bytes()
}
