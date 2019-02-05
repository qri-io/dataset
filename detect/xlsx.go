package detect

import (
	"io"

	"github.com/qri-io/dataset"
)

// XLSXSchema determines any schema information for an excel spreadsheet
// TODO (b5): currently unimplemented
func XLSXSchema(r *dataset.Structure, data io.Reader) (schema map[string]interface{}, n int, err error) {
	return dataset.BaseSchemaArray, 0, nil
}
