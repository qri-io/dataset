package detect

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

var (
	// BaseSchemaJSONArray is a minimum schema for the json file format, specifying that the top
	// level of the document is an array
	BaseSchemaJSONArray = jsonschema.Must(`{"type":"array"}`)
	// BaseSchemaJSONObject is a minimum schema for the json format, specifying that the top level
	// of the document is an object
	BaseSchemaJSONObject = jsonschema.Must(`{"type":"object"}`)
)

// JSONSchema determines the field names and types of an io.Reader of JSON-formatted data, returning a json schema
func JSONSchema(resource *dataset.Structure, data io.Reader) (schema *jsonschema.RootSchema, err error) {
	rd := bufio.NewReader(data)
	lin, err := rd.ReadSlice('{')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading data: %s", err.Error())
	}

	if bytes.Contains(lin, []byte{'['}) {
		return BaseSchemaJSONArray, nil
	}

	return BaseSchemaJSONObject, nil
}
