package dsio

import (
	"bytes"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
)

// ValueBuffer mimics the behaviour of bytes.Buffer, but with structured Dataa
// Read and Write are replaced with ReadRow and WriteValue. It's worth noting
// that different data formats have idisyncrcies that affect the behavior
// of buffers and their output. For example, ValueBuffer won't write things like
// CSV header rows or enclosing JSON arrays until after the writer's
// Close method has been called.
type ValueBuffer struct {
	structure *dataset.Structure
	r         ValueReader
	w         ValueWriter
	buf       *bytes.Buffer
}

// NewValueBuffer allocates a buffer, buffers should always be created with
// NewValueBuffer, which will error if the provided structure is invalid for
// reading / writing
func NewValueBuffer(st *dataset.Structure) (*ValueBuffer, error) {
	buf := &bytes.Buffer{}
	r, err := NewValueReader(st, buf)
	if err != nil {
		return nil, err
	}
	w, err := NewValueWriter(st, buf)
	if err != nil {
		return nil, err
	}

	return &ValueBuffer{
		structure: st,
		r:         r,
		w:         w,
		buf:       buf,
	}, nil
}

// Structure gives the underlying structure this buffer is using
func (b *ValueBuffer) Structure() *dataset.Structure {
	return b.structure
}

// ReadRow reads one row from the buffer
func (b *ValueBuffer) ReadRow() (vals.Value, error) {
	return b.r.ReadValue()
}

// WriteValue writes one row to the buffer
func (b *ValueBuffer) WriteValue(val vals.Value) error {
	return b.w.WriteValue(val)
}

// Close closes the writer portion of the buffer, which will affect
// underlying contents.
func (b *ValueBuffer) Close() error {
	return b.w.Close()
}

// Bytes gives the raw contents of the underlying buffer
func (b *ValueBuffer) Bytes() []byte {
	return b.buf.Bytes()
}
