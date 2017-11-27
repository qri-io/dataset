package dsio

import (
	"bytes"

	"github.com/qri-io/dataset"
)

// StructuredBuffer mimics the behaviour of bytes.Buffer, but with structured Dataa
// Read and Write are replaced with ReadRow and WriteRow. It's worth noting
// that different data formats have idisyncrcies that affect the behavior
// of buffers and their output. For example, StructuredBuffer won't write things like
// CSV header rows or enclosing JSON arrays until after the writer's
// Close method has been called.
type StructuredBuffer struct {
	structure *dataset.Structure
	r         RowReader
	w         RowWriter
	buf       *bytes.Buffer
}

// NewStructuredBuffer allocates a buffer, buffers should always be created with
// NewStructuredBuffer, which will error if the provided structure is invalid for
// reading / writing
func NewStructuredBuffer(st *dataset.Structure) (*StructuredBuffer, error) {
	buf := &bytes.Buffer{}
	r, err := NewRowReader(st, buf)
	if err != nil {
		return nil, err
	}
	w, err := NewRowWriter(st, buf)
	if err != nil {
		return nil, err
	}

	return &StructuredBuffer{
		structure: st,
		r:         r,
		w:         w,
		buf:       buf,
	}, nil
}

// Structure gives the underlying structure this buffer is using
func (b *StructuredBuffer) Structure() *dataset.Structure {
	return b.structure
}

// ReadRow reads one row from the buffer
func (b *StructuredBuffer) ReadRow() ([][]byte, error) {
	return b.r.ReadRow()
}

// WriteRow writes one row to the buffer
func (b *StructuredBuffer) WriteRow(row [][]byte) error {
	return b.w.WriteRow(row)
}

// Close closes the writer portion of the buffer, which will affect
// underlying contents.
func (b *StructuredBuffer) Close() error {
	return b.w.Close()
}

// Bytes gives the raw contents of the underlying buffer
func (b *StructuredBuffer) Bytes() []byte {
	return b.buf.Bytes()
}
