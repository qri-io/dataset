package detect

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/compression"
	"github.com/qri-io/qfs"
)

var (
	spaces   = regexp.MustCompile(`[\s-]+`)
	nonAlpha = regexp.MustCompile(`[^a-zA-z0-9_]`)
	log      = logger.Logger("detect")
)

// Structure examines the contents of a dataset body, setting any missing
// elements of a structure component required to make the dataset readable.
// A minimum structure component has non-zero Format and Schema fields, and
// may need additional FormatConfig settings to parse properly.
// Structure will not mutate any component fields that are not a default value
func Structure(ds *dataset.Dataset) error {
	if ds == nil {
		return fmt.Errorf("empty dataset")
	}

	// fast path if nothing needs to be inferred
	if ds.Structure != nil && ds.Structure.Format != "" &&
		ds.Structure.Schema != nil &&
		!needsFormatConfig(ds.Structure) {
		return nil
	}

	body := ds.BodyFile()
	if body == nil {
		return dataset.ErrNoBody
	}
	// use a TeeReader that writes to a buffer to preserve data
	buf := &bytes.Buffer{}
	tr := io.TeeReader(body, buf)
	var df dataset.DataFormat

	df, comp, err := FormatFromFilename(body.FileName())
	if err != nil {
		log.Debug(err.Error())
		return fmt.Errorf("invalid data format: %w", err)
	}

	guessedStructure, _, err := FromReader(df, comp, tr)
	if err != nil {
		log.Debug(err.Error())
		return fmt.Errorf("determining dataset structure: %w", err)
	}

	// attach the structure, schema, and formatConfig, as appropriate
	if ds.Structure == nil {
		ds.Structure = guessedStructure
	}
	if ds.Structure.Format == "" {
		ds.Structure.Format = guessedStructure.Format
	}
	if ds.Structure.Compression == "" {
		ds.Structure.Compression = guessedStructure.Compression
	}
	if ds.Structure.FormatConfig == nil && ds.Structure.Format == guessedStructure.Format {
		ds.Structure.FormatConfig = guessedStructure.FormatConfig
	}
	if ds.Structure.Schema == nil {
		ds.Structure.Schema = guessedStructure.Schema
	}

	// glue whatever we just read back onto the reader
	// TODO (b5)- this may ruin readers that transparently depend on a read-closer
	// we should consider a method on qfs.File that allows this non-destructive read pattern
	size := int64(-1)
	if sizef, ok := body.(qfs.SizeFile); ok {
		size = sizef.Size()
	}
	ds.SetBodyFile(qfs.NewMemfileReaderSize(body.FileName(), io.MultiReader(buf, body), size))
	return nil
}

// needsFormatConfig returns true if a given structure needs a FormatConfig
// field and doesn't have one
// This only returns true for formats that are known to need a FormatConfig,
// defaults to false (no FormatConfig required)
func needsFormatConfig(st *dataset.Structure) bool {
	switch st.Format {
	case dataset.XLSXDataFormat.String():
		// XLSX should always have a format config
		return st.FormatConfig == nil
	case dataset.CSVDataFormat.String():
		// CSVs should always have a format config
		return st.FormatConfig == nil
	case dataset.JSONDataFormat.String():
		// JSON doesn't need a format config
		return false
	default:
		return false
	}
}

// FromFile takes a filepath & tries to work out the corresponding dataset
// for the sake of speed, it only works with files that have a recognized extension
func FromFile(path string) (st *dataset.Structure, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	format, comp, err := FormatFromFilename(path)
	if err != nil {
		return nil, err
	}

	st, _, err = FromReader(format, comp, f)
	return st, err
}

// FromReader detects a dataset structure from a reader and data format, returning a detected dataset
// structure, the number of bytes read from the reader, and any error
func FromReader(format dataset.DataFormat, comp compression.Format, data io.Reader) (st *dataset.Structure, n int, err error) {
	st = &dataset.Structure{
		Format:      format.String(),
		Compression: comp.String(),
	}
	st.Schema, n, err = Schema(st, data)
	return
}

// FormatFromFilename extracts data & compression formats from a filename string
// by examining file extensions. Assumes that when multiple extensions are
// present they come in the order: filename.[data_format].[compression_format]
func FormatFromFilename(path string) (dataset.DataFormat, compression.Format, error) {
	ext := filepath.Ext(path)

	compFmt, e := compression.ParseFormat(strings.TrimPrefix(ext, "."))
	if e == nil {
		ext = filepath.Ext(strings.TrimSuffix(path, ext))
	} else {
		compFmt = compression.FmtNone
	}

	switch ext {
	case ".cbor":
		return dataset.CBORDataFormat, compFmt, nil
	case ".json":
		return dataset.JSONDataFormat, compFmt, nil
	case ".csv":
		return dataset.CSVDataFormat, compFmt, nil
	case ".xml":
		return dataset.XMLDataFormat, compFmt, nil
	case ".xlsx":
		return dataset.XLSXDataFormat, compFmt, nil
	case ".jsonl":
		return dataset.NDJSONDataFormat, compFmt, nil
	case ".ndjson":
		return dataset.NDJSONDataFormat, compFmt, nil
	case "":
		return dataset.UnknownDataFormat, compFmt, errors.New("no file extension provided")
	default:
		return dataset.UnknownDataFormat, compFmt, fmt.Errorf("unsupported file type: '%s'", ext)
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
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
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
