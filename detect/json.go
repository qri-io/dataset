package detect

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset"
)

// JSONSchema determines the field names and types of an io.Reader of JSON-formatted data, returning a json schema
// This is currently a suuuuuuuuper simple interpretation that spits out a generic schema that'll work. In the future
// we can do all sorts of stuff here to make better inferences about the shape of a dataset, but for now, this'll work,
// and we'll instead focus on making it easier for users to provide hand-built schemas
func JSONSchema(resource *dataset.Structure, data io.Reader) (schema map[string]interface{}, n int, err error) {
	var (
		count = 0
		buf   = make([]byte, 100)
	)

	for {
		count, err = data.Read(buf)
		n += count
		if err != nil {
			if err == io.EOF {
				// possible that data length is less than 100 bytes,
				// if we've read more than 0 bytes, we should check it
				if count > 0 {
					err = nil
				} else {
					err = fmt.Errorf("invalid json data")
					return
				}
			} else {
				log.Debugf(err.Error())
				err = fmt.Errorf("error reading data: %s", err.Error())
				return
			}
		}

		for _, b := range buf {
			switch b {
			case '[':
				return dataset.BaseSchemaArray, n, nil
			case '{':
				return dataset.BaseSchemaObject, n, nil
			case ' ', '\t', '\n', '\r':
				continue
			default:
				err = fmt.Errorf("invalid json data")
				return
			}
		}
	}
}
