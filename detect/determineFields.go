package detect

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/dsio/replacecr"
	"github.com/qri-io/dataset/vals"
	"github.com/qri-io/varName"
)

var (
	startsWithNumberRegex = regexp.MustCompile(`^[0-9]`)
)

// Schema determines the schema of a given reader for a given structure
func Schema(r *dataset.Structure, data io.Reader) (schema map[string]interface{}, n int, err error) {
	if r.DataFormat() == dataset.UnknownDataFormat {
		err = fmt.Errorf("dataset format must be specified to determine schema")
		log.Infof(err.Error())
		return
	}

	switch r.DataFormat() {
	case dataset.CBORDataFormat:
		return CBORSchema(r, data)
	case dataset.JSONDataFormat:
		return JSONSchema(r, data)
	case dataset.CSVDataFormat:
		return CSVSchema(r, data)
	case dataset.XLSXDataFormat:
		return XLSXSchema(r, data)
	default:
		err = fmt.Errorf("'%s' is not supported for field detection", r.Format)
		return
	}
}

type field struct {
	Title string    `json:"title,omitempty"`
	Type  vals.Type `json:"type,omitempty"`
}

// CSVSchema determines the field names and types of an io.Reader of CSV-formatted data, returning a json schema
func CSVSchema(resource *dataset.Structure, data io.Reader) (schema map[string]interface{}, n int, err error) {
	tr := dsio.NewTrackedReader(data)
	r := csv.NewReader(replacecr.Reader(tr))
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	r.LazyQuotes = true

	opt := map[string]interface{}{
		// TODO - for now we're going to assume lazy quotes. we should scan the entire file
		// for unescaped quotes & only set this to true if that's the case.
		"lazyQuotes": true,
	}
	resource.FormatConfig = opt

	header, err := r.Read()
	if err != nil {
		return nil, tr.BytesRead(), err
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
		opt["headerRow"] = true
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
			return nil, tr.BytesRead(), fmt.Errorf("error reading csv file: %s", err.Error())
		}

		if len(rec) == len(types) {
			for i, cell := range rec {
				types[i][vals.ParseType([]byte(cell))]++
			}
			count++
		} else {
			opt["variadicFields"] = true
		}
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
		return nil, tr.BytesRead(), fmt.Errorf("error marshaling csv fields to json: %s", err.Error())
	}
	schstr := fmt.Sprintf(`{"type":"array","items":{"type":"array","items":%s}}`, string(items))

	sch := map[string]interface{}{}
	if err := json.Unmarshal([]byte(schstr), &sch); err != nil {
		return nil, tr.BytesRead(), err
	}

	return sch, tr.BytesRead(), nil
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
