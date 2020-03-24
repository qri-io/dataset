package detect

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
)

var (
	spaces   = regexp.MustCompile(`[\s-]+`)
	nonAlpha = regexp.MustCompile(`[^a-zA-z0-9_]`)
	log      = logger.Logger("detect")
)

// FromFile takes a filepath & tries to work out the corresponding dataset
// for the sake of speed, it only works with files that have a recognized extension
func FromFile(path string) (st *dataset.Structure, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	format, err := ExtensionDataFormat(path)
	if err != nil {
		return nil, err
	}

	st, _, err = FromReader(format, f)
	return st, err
}

// FromReader detects a dataset structure from a reader and data format, returning a detected dataset
// structure, the number of bytes read from the reader, and any error
func FromReader(format dataset.DataFormat, data io.Reader) (st *dataset.Structure, n int, err error) {
	st = &dataset.Structure{
		Format: format.String(),
	}
	st.Schema, n, err = Schema(st, data)
	return
}

// ExtensionDataFormat returns the corresponding DataFormat for a given file extension if one exists
// TODO - this should probably come from the dataset package
func ExtensionDataFormat(path string) (format dataset.DataFormat, err error) {
	ext := filepath.Ext(path)
	switch ext {
	case ".cbor":
		return dataset.CBORDataFormat, nil
	case ".json":
		return dataset.JSONDataFormat, nil
	case ".csv":
		return dataset.CSVDataFormat, nil
	case ".xml":
		return dataset.XMLDataFormat, nil
	case ".xlsx":
		return dataset.XLSXDataFormat, nil
	case "":
		return dataset.UnknownDataFormat, errors.New("no file extension provided")
	default:
		return dataset.UnknownDataFormat, fmt.Errorf("unsupported file type: '%s'", ext)
	}
}

// ErrInvalidTabularData indicates non-tabular data in a context that expects
// tabular input
var ErrInvalidTabularData = errors.New("invalid tabular data")

// TabularSchemaFromTabularData infers a basic tabular JSON schema from go types
// it only works in the narrow case where the source data is known to be tabular
// but lacks a schema to describe it
// given the lack of metadata, these schema should be used primarily for
// machine purposes
func TabularSchemaFromTabularData(source interface{}) (map[string]interface{}, error) {
	schema := map[string]interface{}{}
	switch data := source.(type) {
	case []interface{}:
		schema["type"] = "array"
		items := map[string]interface{}{}
		if len(data) == 0 {
			return nil, fmt.Errorf("%w: missing row data", ErrInvalidTabularData)
		}

		switch ent := data[0].(type) {
		case []interface{}:
			items["type"] = "array"
			cols := make([]interface{}, len(ent))
			for i, v := range ent {
				cols[i] = map[string]interface{}{
					"title": fmt.Sprintf("col_%d", i),
					"type":  goDataType(v),
				}
			}
			items["items"] = cols
		default:
			return nil, fmt.Errorf("%w: array schemas must use an inner array for rows", ErrInvalidTabularData)
		}
		schema["items"] = items
	case map[string]interface{}:
		return nil, fmt.Errorf("%w: cannot interpret object-based tabular schemas", ErrInvalidTabularData)
	}

	return schema, nil
}

func goDataType(v interface{}) string {
	switch v.(type) {
	case int64, int, uint, float64:
		return "number"
	case bool:
		return "boolean"
	case nil:
		return "null"
	default:
		// assume a string type
		return "string"
	}
}
