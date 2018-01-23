package dsio

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
)

// CSVReader implements the RowReader interface for the CSV data format
type CSVReader struct {
	st         *dataset.Structure
	readHeader bool
	r          *csv.Reader
}

// NewCSVReader creates a reader from a structure and read source
func NewCSVReader(st *dataset.Structure, r io.Reader) *CSVReader {
	return &CSVReader{
		st: st,
		r:  csv.NewReader(r),
	}
}

// Structure gives this reader's structure
func (r *CSVReader) Structure() *dataset.Structure {
	return r.st
}

// ReadValue reads one CSV record from the reader
func (r *CSVReader) ReadValue() (vals.Value, error) {
	if !r.readHeader {
		if HasHeaderRow(r.st) {
			if _, err := r.r.Read(); err != nil {
				if err.Error() == "EOF" {
					return nil, nil
				}
				return nil, err
			}
		}
		r.readHeader = true
	}

	data, err := r.r.Read()
	if err != nil {
		return nil, err
	}
	row := make(vals.Array, len(data))
	for i, d := range data {
		row[i] = vals.String(string(d))
	}
	return row, nil
}

// HasHeaderRow checks Structure for the presence of the HeaderRow flag
func HasHeaderRow(st *dataset.Structure) bool {
	if st.Format == dataset.CSVDataFormat && st.FormatConfig != nil {
		if csvOpt, ok := st.FormatConfig.(*dataset.CSVOptions); ok {
			return csvOpt.HeaderRow
		}
	}
	return false
}

// CSVWriter implements the RowWriter interface for
// CSV-formatted data
type CSVWriter struct {
	rowsWritten int
	w           *csv.Writer
	st          *dataset.Structure
}

// NewCSVWriter creates a Writer from a structure and write destination
func NewCSVWriter(st *dataset.Structure, w io.Writer) *CSVWriter {
	writer := csv.NewWriter(w)
	wr := &CSVWriter{
		st: st,
		w:  writer,
	}

	if CSVOpts, ok := st.FormatConfig.(*dataset.CSVOptions); ok {
		if CSVOpts.HeaderRow {
			// TODO - capture error
			if titles, err := terribleHackToGetHeaderRow(st); err == nil {
				writer.Write(titles)
			}
		}
	}

	return wr
}

// TODO - holy shit dis so bad. fix
func terribleHackToGetHeaderRow(st *dataset.Structure) ([]string, error) {
	data, err := st.Schema.MarshalJSON()
	if err != nil {
		return nil, err
	}
	sch := map[string]interface{}{}
	if err := json.Unmarshal(data, &sch); err != nil {
		return nil, err
	}
	if itemObj, ok := sch["items"].(map[string]interface{}); ok {
		if itemArr, ok := itemObj["items"].([]interface{}); ok {
			titles := make([]string, len(itemArr))
			for i, f := range itemArr {
				if field, ok := f.(map[string]interface{}); ok {
					if title, ok := field["title"].(string); ok {
						titles[i] = title
					}
				}
			}
			return titles, nil
		}
	}
	return nil, fmt.Errorf("nope")
}

// Structure gives this writer's structure
func (w *CSVWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteValue writes one CSV record to the writer
func (w *CSVWriter) WriteValue(val vals.Value) error {
	if arr, ok := val.(vals.Array); ok {
		row := make([]string, len(arr))
		for i, d := range arr {
			row[i] = d.String()
		}
		return w.w.Write(row)
	}
	return fmt.Errorf("expected array value to write csv row. got: %s", val.Type())
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *CSVWriter) Close() error {
	w.w.Flush()
	return nil
}
