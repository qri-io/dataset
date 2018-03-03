package detect

import (
	"bufio"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

const (
	cborBdIndefiniteArray byte = 0x9f
	cborBdIndefiniteMap        = 0xbf
	cborBaseArray              = 0x80
	cborBaseMap                = 0xa0
	cborBaseTag                = 0xc0
)

// CBORSchema determines the field names and types of an io.Reader of CBOR-formatted data, returning a json schema
func CBORSchema(resource *dataset.Structure, data io.Reader) (schema *jsonschema.RootSchema, err error) {
	rd := bufio.NewReader(data)
	bd, err := rd.ReadByte()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading data: %s", err.Error())
	}

	switch {
	case bd >= cborBaseArray && bd < cborBaseMap, bd == cborBdIndefiniteArray:
		return dataset.BaseSchemaArray, nil
	case bd >= cborBaseMap && bd < cborBaseTag, bd == cborBdIndefiniteMap:
		return dataset.BaseSchemaObject, nil
	default:
		return nil, fmt.Errorf("invalid top-level type for CBOR data. cbor datasets must begin with either an array or map")
	}
}
