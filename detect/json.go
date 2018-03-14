package detect

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

// JSONSchema determines the field names and types of an io.Reader of JSON-formatted data, returning a json schema
func JSONSchema(resource *dataset.Structure, data io.Reader) (schema *jsonschema.RootSchema, err error) {
	rd := bufio.NewReader(data)
	lin, err := rd.ReadSlice('{')
	if err != nil && err != io.EOF {
		log.Debugf(err.Error())
		return nil, fmt.Errorf("error reading data: %s", err.Error())
	}

	if bytes.Contains(lin, []byte{'['}) {
		return dataset.BaseSchemaArray, nil
	}

	return dataset.BaseSchemaObject, nil
}
