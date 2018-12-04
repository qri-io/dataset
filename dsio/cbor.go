package dsio

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/qri-io/dataset"
	"github.com/ugorji/go/codec"
)

// CBORReader implements the RowReader interface for the CBOR data format
type CBORReader struct {
	rowsRead int
	rdr      *bufio.Reader
	st       *dataset.Structure
	topLevel byte
}

var (
	bigen = binary.BigEndian
)

// NewCBORReader creates a reader from a structure and read source
func NewCBORReader(st *dataset.Structure, r io.Reader) (*CBORReader, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for CBOR reader")
		log.Debug(err.Error())
		return nil, err
	}

	tlt, err := GetTopLevelType(st)
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}

	var topLevel byte
	topLevel = cborBaseArray
	if tlt == "object" {
		topLevel = cborBaseMap
	}

	return &CBORReader{
		st:       st,
		rdr:      bufio.NewReader(r),
		topLevel: topLevel,
	}, nil
}

// Structure gives this writer's structure
func (r *CBORReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one CBOR record from the reader
func (r *CBORReader) ReadEntry() (ent Entry, err error) {
	if r.rowsRead == 0 {
		top, _, err := r.readTopLevel()
		if err != nil {
			return ent, err
		}
		if top != r.topLevel {
			return ent, fmt.Errorf("Top-level type did not match")
		}
	}

	if r.topLevel == cborBaseMap {
		ent.Key, err = r.readMapKey()
		if err != nil {
			return
		}
	} else {
		ent.Index = r.rowsRead
	}

	ent.Value, err = r.readValue()
	if err != nil {
		return
	}

	r.rowsRead++
	return
}

const (
	cborBdFalse byte = 0xf4 + iota
	cborBdTrue
	cborBdNil
	cborBdUndefined
	cborBdExt
	cborBdFloat16
	cborBdFloat32
	cborBdFloat64
)

const (
	cborBdIndefiniteBytes  byte = 0x5f
	cborBdIndefiniteString      = 0x7f
	cborBdIndefiniteArray       = 0x9f
	cborBdIndefiniteMap         = 0xbf
	cborBdBreak                 = 0xff
)

const (
	cborBaseUint   byte = 0x00
	cborBaseNegInt      = 0x20
	cborBaseBytes       = 0x40
	cborBaseString      = 0x60
	cborBaseArray       = 0x80
	cborBaseMap         = 0xa0
	cborBaseTag         = 0xc0
	cborBaseSimple      = 0xe0
)

const cborTypeMask byte = 0xe0

// readTopLevel determines the top-level type, either "object" or "array"
func (r *CBORReader) readTopLevel() (byte, int, error) {
	b, err := r.rdr.ReadByte()
	if err != nil {
		return 0xff, -1, err
	}

	t := b & cborTypeMask
	if t != cborBaseArray && t != cborBaseMap {
		return 0xff, -1, fmt.Errorf("invalid top level type")
	}

	length, err := r.getVarLenInt(b)
	if err != nil {
		return 0xff, -1, err
	}

	return t, int(length), nil
}

// readMapKey reads a map key "string" from the input stream
func (r *CBORReader) readMapKey() (string, error) {
	b, err := r.rdr.ReadByte()
	if err != nil {
		return "", err
	}

	if b&cborTypeMask != cborBaseString {
		return "", fmt.Errorf("expected string for key")
	}

	length, err := r.getVarLenInt(b)
	if err != nil {
		return "", err
	}

	buff, err := r.rdr.Peek(int(length))
	if err != nil {
		return "", err
	}

	_, err = r.rdr.Discard(int(length))
	if err != nil {
		return "", err
	}

	return string(buff), nil
}

// readValue reads a value of any type from the input stream
func (r *CBORReader) readValue() (interface{}, error) {
	b, err := r.rdr.ReadByte()
	if err != nil {
		return nil, err
	}

	if b < 0x1c {
		return r.getVarLenInt(b)
	} else if b >= 0x20 && b < 0x38 {
		return -int64(b - 0x1f), nil
	}

	switch b {
	case cborBdNil:
		return nil, nil
	case cborBdFalse:
		return false, nil
	case cborBdTrue:
		return true, nil
	case cborBdFloat16:
		return r.readFloatBytes(2)
	case cborBdFloat32:
		return r.readFloatBytes(4)
	case cborBdFloat64:
		return r.readFloatBytes(8)
	case cborBdIndefiniteBytes:
		// TODO: Implement me
		return nil, nil
	case cborBdIndefiniteArray:
		// TODO: Implement me
		return nil, nil
	case cborBdIndefiniteMap:
		// TODO: Implement me
		return nil, nil
	default:
		t := b & cborTypeMask
		switch t {
		case cborBaseBytes:
			// TODO: Implement me
			return nil, nil
		case cborBaseString:
			length, err := r.getVarLenInt(b)
			if err != nil {
				return nil, err
			}
			buff, err := r.rdr.Peek(int(length))
			if err != nil {
				return nil, err
			}

			_, err = r.rdr.Discard(int(length))
			if err != nil {
				return nil, err
			}

			return string(buff), nil
		case cborBaseArray:
			length, err := r.getVarLenInt(b)
			if err != nil {
				return nil, err
			}
			array := make([]interface{}, 0, int(length))
			for i := 0; i < int(length); i++ {
				val, err := r.readValue()
				if err != nil {
					return nil, err
				}
				array = append(array, val)
			}
			return array, nil
		case cborBaseMap:
			length, err := r.getVarLenInt(b)
			if err != nil {
				return nil, err
			}
			assoc := make(map[string]interface{})
			for i := 0; i < int(length); i++ {
				key, err := r.readMapKey()
				if err != nil {
					return nil, err
				}
				val, err := r.readValue()
				if err != nil {
					return nil, err
				}
				assoc[key] = val
			}
			return assoc, nil
		case cborBaseTag:
			// TODO: Implement me
			return nil, nil
		case cborBaseSimple:
			// TODO: Implement me
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown cbor tag: %v", b)
		}
	}
}

// getVarLenInt handles the byte most recently read, and possibly reads more bytes, to get an int
func (r *CBORReader) getVarLenInt(b byte) (int64, error) {
	b = b & 0x1f
	if b < 0x18 {
		return int64(b), nil
	} else if b == 0x18 {
		return r.readIntBytes(1)
	} else if b == 0x19 {
		return r.readIntBytes(2)
	} else if b == 0x1a {
		return r.readIntBytes(4)
	} else if b == 0x1b {
		return r.readIntBytes(8)
	} else {
		return 0, fmt.Errorf("Could not decode variable length int: %v", b)
	}
}

// readIntBytes returns an int by reading num bytes from the input stream
func (r *CBORReader) readIntBytes(num int) (int64, error) {
	data, err := r.rdr.Peek(num)
	if err != nil {
		return 0, err
	}
	_, err = r.rdr.Discard(num)
	if err != nil {
		return 0, err
	}
	if num < 8 {
		data = bytes.Join([][]byte{bytes.Repeat([]byte{0}, 8-len(data)), data}, []byte{})
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

// readFloatBytes returns a float by reading num bytes from the input stream
func (r *CBORReader) readFloatBytes(num int) (float64, error) {
	data, err := r.rdr.Peek(num)
	if err != nil {
		return 0, err
	}
	_, err = r.rdr.Discard(num)
	if err != nil {
		return 0, err
	}
	if num < 8 {
		data = bytes.Join([][]byte{bytes.Repeat([]byte{0}, 8-len(data)), data}, []byte{})
	}
	return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
}

// CBORWriter implements the RowWriter interface for
// CBOR-formatted data
type CBORWriter struct {
	rowsWritten int
	tlt         string
	st          *dataset.Structure
	wr          io.Writer
	arr         []interface{}
	obj         map[string]interface{}
}

// NewCBORWriter creates a Writer from a structure and write destination
func NewCBORWriter(st *dataset.Structure, w io.Writer) (*CBORWriter, error) {
	if st.Schema == nil {
		return nil, fmt.Errorf("schema required for CBOR writer")
	}

	tlt, err := GetTopLevelType(st)
	if err != nil {
		return nil, err
	}
	cw := &CBORWriter{
		st:  st,
		wr:  w,
		tlt: tlt,
	}

	if cw.tlt == "object" {
		cw.obj = map[string]interface{}{}
	} else {
		cw.arr = []interface{}{}
	}

	return cw, nil
}

// Structure gives this writer's structure
func (w *CBORWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteEntry writes one CBOR record to the writer
func (w *CBORWriter) WriteEntry(ent Entry) error {
	defer func() {
		w.rowsWritten++
	}()

	if w.tlt == "object" {
		if ent.Key == "" {
			return fmt.Errorf("Key cannot be empty")
		}

		if _, ok := w.obj[ent.Key]; ok {
			return fmt.Errorf(`key already written: '%s'`, ent.Key)
		}
		w.obj[ent.Key] = ent.Value
		return nil
	}

	w.arr = append(w.arr, ent.Value)
	return nil
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *CBORWriter) Close() error {
	h := &codec.CborHandle{TimeRFC3339: true}
	h.Canonical = true
	enc := codec.NewEncoder(w.wr, h)

	if w.tlt == "object" {
		return enc.Encode(w.obj)
	}

	return enc.Encode(w.arr)
}
