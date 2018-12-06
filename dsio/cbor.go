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
	length   int
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
		top, length, err := r.readTopLevel()
		if err != nil {
			return ent, err
		}
		if top != r.topLevel {
			return ent, fmt.Errorf("Top-level type did not match")
		}
		// TODO: Length is not used right now, except for handling indefinite length streams.
		// In the future, it should be used to check that max(r.rowsRead) == r.length
		r.length = length
	}

	if r.length == indefiniteLength && r.readIndefiniteSequenceBreak() {
		return ent, io.EOF
	}

	if r.topLevel == cborBaseMap {
		ent.Key, err = r.readStringKey()
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

const indefiniteLength int = -1

const cborTypeMask byte = 0xe0

// readTopLevel determines the top-level type, either "object" or "array"
func (r *CBORReader) readTopLevel() (byte, int, error) {
	b, err := r.rdr.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	t := b & cborTypeMask
	if t != cborBaseArray && t != cborBaseMap {
		return 0, 0, fmt.Errorf("invalid top level type")
	}

	// Indefinite size
	if b&0x1f == 0x1f {
		return t, indefiniteLength, err
	}

	length, err := r.getVarLenInt(b)
	if err != nil {
		return 0, 0, err
	}

	return t, int(length), nil
}

// readStringKey reads a key for a map from the input stream
func (r *CBORReader) readStringKey() (string, error) {
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

	buff, err := r.readBytes(int(length))
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
		concat := bytes.Buffer{}
		for {
			if r.readIndefiniteSequenceBreak() {
				break
			}
			b, err := r.rdr.ReadByte()
			if err != nil {
				return nil, err
			}
			buff, err := r.readLengthPrefixedBytes(b)
			if err != nil {
				return nil, err
			}
			concat.Write(buff)
		}
		return concat.Bytes(), nil
	case cborBdIndefiniteString:
		concat := bytes.Buffer{}
		for {
			if r.readIndefiniteSequenceBreak() {
				break
			}
			b, err := r.rdr.ReadByte()
			if err != nil {
				return nil, err
			}
			buff, err := r.readLengthPrefixedBytes(b)
			if err != nil {
				return nil, err
			}
			concat.Write(buff)
		}
		return string(concat.Bytes()), nil
	case cborBdIndefiniteArray:
		array, err := r.readArray(indefiniteLength)
		if err != nil {
			return nil, err
		}
		return array, nil
	case cborBdIndefiniteMap:
		assoc, err := r.readMap(indefiniteLength)
		if err != nil {
			return nil, err
		}
		return assoc, nil
	default:
		t := b & cborTypeMask
		switch t {
		case cborBaseString:
			buff, err := r.readLengthPrefixedBytes(b)
			if err != nil {
				return nil, err
			}
			return string(buff), nil
		case cborBaseBytes:
			buff, err := r.readLengthPrefixedBytes(b)
			if err != nil {
				return nil, err
			}
			return buff, nil
		case cborBaseArray:
			length, err := r.getVarLenInt(b)
			if err != nil {
				return nil, err
			}
			array, err := r.readArray(int(length))
			if err != nil {
				return nil, err
			}
			return array, nil
		case cborBaseMap:
			length, err := r.getVarLenInt(b)
			if err != nil {
				return nil, err
			}
			assoc, err := r.readMap(int(length))
			if err != nil {
				return nil, err
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

// readIndefiniteSequenceBreak returns true if the next byte is a sequence break
func (r *CBORReader) readIndefiniteSequenceBreak() bool {
	bytes, err := r.rdr.Peek(1)
	if err != nil {
		return false
	}
	if bytes[0] == 0xff {
		_, _ = r.rdr.Discard(1)
		return true
	}
	return false
}

// readLengthPrefixedBytes returns a number of bytes prefixed by the number of bytes to read
func (r *CBORReader) readLengthPrefixedBytes(b byte) ([]byte, error) {
	length, err := r.getVarLenInt(b)
	if err != nil {
		return nil, err
	}
	buff, err := r.readBytes(int(length))
	if err != nil {
		return nil, err
	}
	return buff, nil
}

// readArray reads an array of the given length
func (r *CBORReader) readArray(length int) ([]interface{}, error) {
	var array []interface{}
	if length > 0 {
		array = make([]interface{}, 0, length)
	} else {
		array = make([]interface{}, 0)
	}
	for {
		if length == 0 || (length == indefiniteLength && r.readIndefiniteSequenceBreak()) {
			break
		}
		val, err := r.readValue()
		if err != nil {
			return nil, err
		}
		array = append(array, val)
		if length > 0 {
			length--
		}
	}
	return array, nil
}

// readArray reads a map of the given length
func (r *CBORReader) readMap(length int) (map[string]interface{}, error) {
	assoc := make(map[string]interface{})
	for {
		if length == 0 || (length == indefiniteLength && r.readIndefiniteSequenceBreak()) {
			break
		}
		key, err := r.readStringKey()
		if err != nil {
			return nil, err
		}
		val, err := r.readValue()
		if err != nil {
			return nil, err
		}
		assoc[key] = val
		if length > 0 {
			length--
		}
	}
	return assoc, nil
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
	data, err := r.readBytes(num)
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
	data, err := r.readBytes(num)
	if err != nil {
		return 0.0, err
	}
	if num < 8 {
		data = bytes.Join([][]byte{bytes.Repeat([]byte{0}, 8-len(data)), data}, []byte{})
	}
	return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
}

// readBytes reads a number of bytes from the input stream
func (r *CBORReader) readBytes(num int) ([]byte, error) {
	buff, err := r.rdr.Peek(num)
	if err != nil {
		return nil, err
	}
	_, err = r.rdr.Discard(num)
	if err != nil {
		return nil, err
	}
	return buff, nil
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
