package detect

import (
	"bufio"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
)

const (
	cborBdIndefiniteArray byte = 0x9f
	cborBdIndefiniteMap        = 0xbf
	cborBaseArray              = 0x80
	cborBaseMap                = 0xa0
	cborBaseTag                = 0xc0
)

// CBORSchema determines the field names and types of an io.Reader of CBOR-formatted data, returning a json schema
func CBORSchema(resource *dataset.Structure, data io.Reader) (schema map[string]interface{}, n int, err error) {
	rd := bufio.NewReader(data)
	bd, err := rd.ReadByte()
	n++
	if err != nil && err != io.EOF {
		log.Debugf(err.Error())
		err = fmt.Errorf("error reading data: %s", err.Error())
		return
	}

	switch {
	case bd >= cborBaseArray && bd < cborBaseMap, bd == cborBdIndefiniteArray:
		return dataset.BaseSchemaArray, n, nil
	case bd >= cborBaseMap && bd < cborBaseTag, bd == cborBdIndefiniteMap:
		return dataset.BaseSchemaObject, n, nil
	default:
		err = fmt.Errorf("invalid top-level type for CBOR data. cbor datasets must begin with either an array or map")
		log.Debugf(err.Error())
		return
	}
}
