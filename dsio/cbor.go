package dsio

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/qri-io/dataset"
	"github.com/ugorji/go/codec"
)

// CBORReader implements the RowReader interface for the CBOR data format
type CBORReader struct {
	rowsRead   int
	depth      int
	rdr        *bufio.Reader
	st         *dataset.Structure
	token      *bytes.Buffer
	readingMap bool
	sm         scanMode
	handle     *codec.CborHandle
}

var (
	bigen = binary.BigEndian
)

// NewCBORReader creates a reader from a structure and read source
func NewCBORReader(st *dataset.Structure, r io.Reader) (*CBORReader, error) {
	if st.Schema == nil {
		return nil, fmt.Errorf("schema required for CBOR reader")
	}

	sm, err := schemaScanMode(st.Schema)
	if err != nil {
		return nil, err
	}

	return &CBORReader{
		st:    st,
		rdr:   bufio.NewReader(r),
		token: &bytes.Buffer{},
		sm:    sm,
		handle: &codec.CborHandle{
			TimeRFC3339: true,
			BasicHandle: codec.BasicHandle{
				DecodeOptions: codec.DecodeOptions{
					MapType:       reflect.TypeOf(map[string]interface{}{}),
					SignedInteger: true,
				},
			},
		},
	}, nil
}

// Structure gives this writer's structure
func (r *CBORReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one CBOR record from the reader
func (r *CBORReader) ReadEntry() (ent Entry, err error) {
	if r.rowsRead == 0 {
		if _, err = r.readTopLevel(); err != nil {
			r.rowsRead++
			return
		}
	}
	r.rowsRead++

	if r.readingMap {
		err = r.decodeToken(&ent.Key)
		if err != nil {
			return
		}
	}

	err = r.decodeToken(&ent.Value)
	return
}

const (
	cborMajorUint byte = iota
	cborMajorNegInt
	cborMajorBytes
	cborMajorText
	cborMajorArray
	cborMajorMap
	cborMajorTag
	cborMajorOther
)

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
	cborStreamBytes  byte = 0x5f
	cborStreamString      = 0x7f
	cborStreamArray       = 0x9f
	cborStreamMap         = 0xbf
	cborStreamBreak       = 0xff
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

func (r *CBORReader) readTopLevel() (int, error) {
	defer func() {
		r.token.Reset()
	}()

	bd, err := r.rdr.ReadByte()
	if err != nil {
		return 0, err
	}

	// bd = bd & 0x1f
	switch {
	case bd >= cborBaseArray && bd < cborBaseMap, bd == cborBdIndefiniteArray:
		return r.tokAdduInt(bd)
	case bd >= cborBaseMap && bd < cborBaseTag, bd == cborBdIndefiniteMap:
		r.readingMap = true
		return r.tokAdduInt(bd)
	}

	return 0, fmt.Errorf("invalid top level type")
}

func (r *CBORReader) decodeToken(dst interface{}) (err error) {
	if err = r.readToken(); err != nil {
		return
	}

	if err = codec.NewDecoderBytes(r.token.Bytes(), r.handle).Decode(dst); err != nil {
		return
	}

	r.token.Reset()
	return
}

func (r *CBORReader) readToken() error {

	bd, err := r.rdr.ReadByte()
	if err != nil {
		return err
	}

	r.token.WriteByte(bd)

	switch bd {
	case cborBdNil, cborBdFalse, cborBdTrue:
		return nil
	case cborBdFloat16:
		return r.tokAdd(1)
	case cborBdFloat32:
		return r.tokAdd(4)
	case cborBdFloat64:
		return r.tokAdd(8)
	case cborBdIndefiniteBytes:
	case cborBdIndefiniteString:
		// n.s = d.DecodeString()
	case cborBdIndefiniteArray:
		// decodeFurther = true
	case cborBdIndefiniteMap:
		// n.v = valueTypeMap
		// decodeFurther = true
	default:
		switch {
		case bd >= cborBaseUint && bd < cborBaseNegInt, bd >= cborBaseNegInt && bd < cborBaseBytes:
			_, err := r.tokAdduInt(bd)
			return err

		case bd >= cborBaseBytes && bd < cborBaseString, bd >= cborBaseString && bd < cborBaseArray:
			l, err := r.tokAdduInt(bd)
			if err != nil {
				return err
			}
			return r.tokAdd(l)
		case bd >= cborBaseArray && bd < cborBaseMap:
			count, err := r.tokAdduInt(bd)
			if err != nil {
				return err
			}
			for i := 0; i < count; i++ {
				err := r.readToken()
				if err != nil {
					return err
				}
			}
			return nil

		case bd >= cborBaseMap && bd < cborBaseTag:
			count, err := r.tokAdduInt(bd)
			if err != nil {
				return err
			}
			for i := 0; i < count; i++ {
				// read key
				if err := r.readToken(); err != nil {
					return err
				}

				// read value
				if err := r.readToken(); err != nil {
					return err
				}
			}
			return nil

		case bd >= cborBaseTag && bd < cborBaseSimple:
			// TODO
			// n.v = valueTypeExt
			// n.u = d.decUint()
			// n.l = nil
			// if n.u == 0 || n.u == 1 {
			// 	bdRead = false
			// 	// n.v = valueTypeTime
			// 	// n.t = d.decodeTime(n.u)
			// }
			// bdRead = false
			// d.d.decode(&re.Value) // handled by decode itself.
			// decodeFurther = true
			return fmt.Errorf("cbor decoding currently doesn't support custom tags")
		default:
			return fmt.Errorf("unrecognized cbor byte descriptor: 0x%x", bd)
		}
	}

	// if !decodeFurther {
	// 	// bdRead = false
	// }
	panic("boo")
	return fmt.Errorf("booo")
}

// tokAdd transfers i bytes to the token buffer from the reader, returning
// the read byte slice
func (r *CBORReader) tokAdd(i int) error {
	// TODO - slow. make not slow.
	p := make([]byte, i)
	if _, err := r.rdr.Read(p); err != nil {
		return err
	}
	_, err := r.token.Write(p)
	return err
}

// tokAddB transfers i bytes to the token buffer from the reader, returning
// the read byte slice
func (r *CBORReader) tokAddB(i int) ([]byte, error) {
	// TODO - slow. make not slow. only you can prevent unnecessary allocations
	p := make([]byte, i)
	if _, err := r.rdr.Read(p); err != nil {
		return p, err
	}
	_, err := r.token.Write(p)
	return p, err
}

// tokAdduInt writes any necessary tokens to the buffer, returning the read unsigned int
func (r *CBORReader) tokAdduInt(bd byte) (i int, err error) {
	var b []byte
	v := bd & 0x1f
	if v <= 0x17 {
		i = int(v)
		return
	}
	if v == 0x18 {
		bt, e := r.tokAddB(1)
		if e != nil {
			return 0, e
		}
		i = int(bt[0])
		return i, e
	} else if v == 0x19 {
		b, err = r.tokAddB(2)
		i = int(bigen.Uint16(b))
	} else if v == 0x1a {
		b, err = r.tokAddB(4)
		i = int(bigen.Uint32(b))
	} else if v == 0x1b {
		b, err = r.tokAddB(8)
		i = int(bigen.Uint64(b))
	}

	return
}

// CBORWriter implements the RowWriter interface for
// CBOR-formatted data
type CBORWriter struct {
	rowsWritten int
	scanMode    scanMode
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

	cw := &CBORWriter{
		st: st,
		wr: w,
	}

	sm, err := schemaScanMode(st.Schema)
	cw.scanMode = sm
	if sm == smObject {
		cw.obj = map[string]interface{}{}
	} else {
		cw.arr = []interface{}{}
	}

	return cw, err
}

// Structure gives this writer's structure
func (w *CBORWriter) Structure() *dataset.Structure {
	return w.st
}

// ContainerType gives weather this writer is writing an array or an object
func (w *CBORWriter) ContainerType() string {
	if w.scanMode == smObject {
		return "object"
	}
	return "array"
}

// WriteEntry writes one CBOR record to the writer
func (w *CBORWriter) WriteEntry(ent Entry) error {
	defer func() {
		w.rowsWritten++
	}()

	if w.scanMode == smObject {
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

	if w.scanMode == smObject {
		return enc.Encode(w.obj)
	}

	return enc.Encode(w.arr)
}
