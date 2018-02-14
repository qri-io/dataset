package detect

import (
	"bufio"
	"bytes"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

var (
	stdArraySchema  = jsonschema.Must(`{"type":"array"}`)
	stdObjectSchema = jsonschema.Must(`{"type":"object"}`)
)

// JSONSchema determines the field names and types of an io.Reader of JSON-formatted data, returning a json schema
func JSONSchema(resource *dataset.Structure, data io.Reader) (schema *jsonschema.RootSchema, err error) {
	rd := bufio.NewReader(data)
	lin, err := rd.ReadSlice('{')
	if err != nil {
		return nil, err
	}

	if bytes.Contains(lin, []byte{'['}) {
		return stdArraySchema, nil
	}

	return stdObjectSchema, nil
}
