package detect

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

// JSONSchema determines the field names and types of an io.Reader of JSON-formatted data, returning a json schema
// This is currently a suuuuuuuuper simple interpretation that spits out a generic schema that'll work. In the future
// we can do all sorts of stuff here to make better inferences about the shape of a dataset, but for now, this'll work,
// and we'll instead focus on making it easier for users to provide hand-built schemas
func JSONSchema(resource *dataset.Structure, data io.Reader) (schema *jsonschema.RootSchema, err error) {
	buf := make([]byte, 100)
	for {
		if _, err := data.Read(buf); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("invalid json data")
			}
			log.Debugf(err.Error())
			return nil, fmt.Errorf("error reading data: %s", err.Error())
		}

		for _, b := range buf {
			switch b {
			case '[':
				return dataset.BaseSchemaArray, nil
			case '{':
				return dataset.BaseSchemaObject, nil
			case ' ', '\t', '\n', '\r':
				continue
			default:
				return nil, fmt.Errorf("invalid json data")
			}
		}
	}
}
