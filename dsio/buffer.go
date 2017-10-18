package dsio

import (
	"bytes"
	"github.com/qri-io/dataset"
)

func NewBuffer(st *dataset.Structure) *Buffer {
	buf := &bytes.Buffer{}
	return &Buffer{
		structure: st,
		r:         NewReader(st, buf),
		w:         NewWriter(st, buf),
		buf:       buf,
	}
}

type Buffer struct {
	structure *dataset.Structure
	r         Reader
	w         Writer
	buf       *bytes.Buffer
}

func (b *Buffer) ReadRow() ([][]byte, error) {
	return b.r.ReadRow()
}

func (b *Buffer) WriteRow(row [][]byte) error {
	return b.w.WriteRow(row)
}

func (b *Buffer) Bytes() []byte {
	return b.buf.Bytes()
}
