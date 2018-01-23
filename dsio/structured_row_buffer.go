package dsio

// import (
// 	"bytes"
// 	"fmt"
// 	"sort"

// 	"github.com/qri-io/dataset"
// )

// // StructuredRowBuffer is the full-featured version of StructuredBuffer
// // While incurring additional overhead & programmatic completexity, it brings
// // the capactity to do things like sort data by fields, filter duplicate
// // rows, etc.
// type StructuredRowBuffer struct {
// 	st     *dataset.Structure
// 	rows   [][][]byte
// 	less   *func(i, j int) bool
// 	unique bool
// 	err    error
// }

// // StructuredRowBufferCfg encapsulates configuration for StructuredRowBuffer
// type StructuredRowBufferCfg struct {
// 	// TODO - restore
// 	// OrderBy gives a list of orders
// 	// OrderBy []*dataset.Field
// 	// OrderByDesc reverses the order given
// 	// OrderByDesc bool

// 	// Unique silently rejects writing rows
// 	// already present in the buffer
// 	Unique bool
// 	// TODO - FilterFunc only allows rows that pass a given test function
// 	// FilterFunc func(row [][]byte) bool
// }

// // NewStructuredRowBuffer allocates a StructuredRowBuffer from a statement
// func NewStructuredRowBuffer(st *dataset.Structure, configs ...func(o *StructuredRowBufferCfg)) (*StructuredRowBuffer, error) {
// 	cfg := &StructuredRowBufferCfg{}
// 	for _, config := range configs {
// 		config(cfg)
// 	}

// 	rb := &StructuredRowBuffer{
// 		st:     st,
// 		unique: cfg.Unique,
// 	}
// 	// rb.less, rb.err = rb.makeLessFunc(st, cfg)

// 	return rb, nil
// }

// // Structure gives the underlying structure this buffer is using
// func (rb *StructuredRowBuffer) Structure() *dataset.Structure {
// 	return rb.st
// }

// // ReadRow reads one row from the buffer
// func (rb *StructuredRowBuffer) ReadRow() ([][]byte, error) {
// 	return nil, fmt.Errorf("cannot read rows from a *StructuredRowBuffer, call Close() then Bytes() instead")
// }

// // WriteRow writes one row to the buffer
// func (rb *StructuredRowBuffer) WriteRow(row [][]byte) error {
// 	if rb.unique && rb.HasRow(row) {
// 		return nil
// 	}
// 	rb.rows = append(rb.rows, row)
// 	return nil
// }

// // Close closes the writer portion of the buffer, which will affect
// // underlying contents.
// func (rb *StructuredRowBuffer) Close() error {
// 	if rb.err != nil {
// 		return rb.err
// 	}
// 	if rb.less != nil {
// 		sort.Sort(rb)
// 	}
// 	return nil
// }

// // Bytes gives the raw contents of the underlying buffer
// func (rb *StructuredRowBuffer) Bytes() []byte {
// 	buf, err := NewValueBuffer(rb.Structure())
// 	if err != nil {
// 		// shouldn't be possible
// 		panic(err)
// 	}
// 	for _, row := range rb.rows {
// 		if err := buf.WriteRow(row); err != nil {
// 			return nil
// 		}
// 	}
// 	if err := buf.Close(); err != nil {
// 		return nil
// 	}
// 	return buf.Bytes()
// }

// // HasRow checks if a row is in the buffer or not
// func (rb *StructuredRowBuffer) HasRow(row [][]byte) bool {
// ROWS:
// 	for _, r := range rb.rows {
// 		if len(r) != len(row) {
// 			return false
// 		}
// 		for i, cell := range row {
// 			if !bytes.Equal(r[i], cell) {
// 				continue ROWS
// 			}
// 		}
// 		return true
// 	}
// 	return false
// }

// // Len is the number of elements in the collection.
// func (rb *StructuredRowBuffer) Len() int {
// 	return len(rb.rows)
// }

// // Less reports whether the element with
// // index i should sort before the element with index j.
// func (rb *StructuredRowBuffer) Less(i, j int) bool {
// 	less := *rb.less
// 	return less(i, j)
// }

// // Swap swaps the elements with indexes i and j.
// func (rb *StructuredRowBuffer) Swap(i, j int) {
// 	rb.rows[i], rb.rows[j] = rb.rows[j], rb.rows[i]
// }

// // func (rb *StructuredRowBuffer) makeLessFunc(st *dataset.Structure, cfg *StructuredRowBufferCfg) (*func(i, j int) bool, error) {
// // 	if len(cfg.OrderBy) == 0 {
// // 		return nil, nil
// // 	}

// // 	type order struct {
// // 		idx  int
// // 		desc bool
// // 		dt   datatypes.Type
// // 	}

// // 	if st.Schema == nil {
// // 		return nil, fmt.Errorf("structure has no schema")
// // 	}

// // 	orders := []order{}
// // 	for _, o := range cfg.OrderBy {
// // 		idx := -1
// // 		for i, f := range st.Schema.Fields {
// // 			if f == o || f.Name == o.Name {
// // 				idx = i
// // 				break
// // 			}
// // 		}

// // 		if idx < 0 {
// // 			return nil, fmt.Errorf("couldn't find sort field: %s", o.Name)
// // 		}

// // 		orders = append(orders, order{
// // 			idx:  idx,
// // 			desc: cfg.OrderByDesc,
// // 			dt:   st.Schema.Fields[idx].Type,
// // 		})
// // 	}

// // 	less := func(i, j int) bool {
// // 		for _, o := range orders {
// // 			l, err := datatypes.CompareTypeBytes(rb.rows[i][o.idx], rb.rows[j][o.idx], o.dt)
// // 			if err != nil {
// // 				// TODO - wut
// // 				continue
// // 			}
// // 			if l == 0 {
// // 				continue
// // 			}
// // 			return l < 0
// // 		}
// // 		return false
// // 	}

// // 	if cfg.OrderByDesc {
// // 		opposite := func(i, j int) bool { return !less(i, j) }
// // 		return &opposite, nil
// // 	}

// // 	return &less, nil
// // }
