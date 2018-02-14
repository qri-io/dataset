package detect

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
	"github.com/qri-io/jsonschema"
	"github.com/qri-io/varName"
)

var (
	startsWithNumberRegex = regexp.MustCompile(`^[0-9]`)
)

// Schema determines the schema of a given reader for a given structure
func Schema(r *dataset.Structure, data io.Reader) (schema *jsonschema.RootSchema, err error) {
	if r.Format == dataset.UnknownDataFormat {
		return nil, errors.New("dataset format must be specified to determine schema")
	}

	switch r.Format {
	case dataset.CSVDataFormat:
		return CSVSchema(r, data)
	case dataset.JSONDataFormat:
		return JSONSchema(r, data)
	default:
		return nil, fmt.Errorf("'%s' is not supported for field detection", r.Format.String())
	}
}

type field struct {
	Title string    `json:"title,omitempty"`
	Type  vals.Type `json:"type,omitempty"`
}

// CSVSchema determines the field names and types of an io.Reader of CSV-formatted data, returning a json schema
func CSVSchema(resource *dataset.Structure, data io.Reader) (schema *jsonschema.RootSchema, err error) {
	r := csv.NewReader(data)
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	header, err := r.Read()
	if err != nil {
		return nil, err
	}

	fields := make([]*field, len(header))
	types := make([]map[vals.Type]int, len(header))

	for i := range fields {
		fields[i] = &field{
			Title: fmt.Sprintf("field_%d", i+1),
			Type:  vals.TypeUnknown,
		}
		types[i] = map[vals.Type]int{}
	}

	if possibleCsvHeaderRow(header) {
		for i, f := range fields {
			f.Title = varName.CreateVarNameFromString(header[i])
			f.Type = vals.TypeUnknown
		}
		resource.FormatConfig = &dataset.CSVOptions{
			HeaderRow: true,
		}
		// ds.HeaderRow = true
	} else {
		for i, cell := range header {
			types[i][vals.ParseType([]byte(cell))]++
		}
	}

	count := 0
	for {
		rec, err := r.Read()
		// max out at 2000 reads
		if count > 2000 {
			break
		}
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("error reading csv file: %s", err.Error())
		}

		for i, cell := range rec {
			types[i][vals.ParseType([]byte(cell))]++
		}

		count++
	}

	for i, tally := range types {
		for typ, count := range tally {
			if count > tally[fields[i].Type] {
				fields[i].Type = typ
			}
		}
	}

	// TODO - lol what a hack. fix everything, put it in jsonschema.
	items, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("error marshaling csv fields to json: %s", err.Error())
	}
	schstr := fmt.Sprintf(`{"type":"array","items":{"type":"array","items":%s}}`, string(items))

	rs := &jsonschema.RootSchema{}
	if err := rs.UnmarshalJSON([]byte(schstr)); err != nil {
		return nil, err
	}

	return rs, nil
}

// PossibleHeaderRow makes an educated guess about weather or not this csv file has a header row.
// If this returns true, a determination about weather this data contains a header row should be
// made by comparing with the destination schema.
// This is because it's not totally possible to determine if csv data has a header row based on the
// data alone.
// For example, if all columns are a string data type, and all fields in the first row
// are provided, it isn't possible to distinguish between a header row and an entry
func possibleCsvHeaderRow(header []string) bool {
	for _, rawCol := range header {
		col := strings.TrimSpace(rawCol)
		if _, err := vals.ParseInteger([]byte(col)); err == nil {
			// if the row contains valid numeric data, we out.
			return false
		} else if _, err := vals.ParseNumber([]byte(col)); err == nil {
			return false
		} else if col == "" {
			// empty columns can't be headers
			return false
		} else if col == "true" || col == "false" {
			// true & false are keywords, and cannot be headers
			return false
		}
	}
	return true
}
