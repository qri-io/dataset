package dsio

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
)

// CSVReader implements the RowReader interface for the CSV data format
type CSVReader struct {
	st         *dataset.Structure
	readHeader bool
	r          *csv.Reader
	types      []string
}

// NewCSVReader creates a reader from a structure and read source
func NewCSVReader(st *dataset.Structure, r io.Reader) *CSVReader {
	// TODO - handle error
	_, types, _ := terribleHackToGetHeaderRowAndTypes(st)

	return &CSVReader{
		st:    st,
		r:     csv.NewReader(ReplaceSoloCarriageReturns(r)),
		types: types,
	}
}

// Structure gives this reader's structure
func (r *CSVReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one CSV record from the reader
func (r *CSVReader) ReadEntry() (Entry, error) {
	if !r.readHeader {
		if HasHeaderRow(r.st) {
			if _, err := r.r.Read(); err != nil {
				if err.Error() == "EOF" {
					return Entry{}, nil
				}
				return Entry{}, err
			}
		}
		r.readHeader = true
	}

	data, err := r.r.Read()
	if err != nil {
		return Entry{}, err
	}

	value, err := r.decode(data)
	if err != nil {
		return Entry{}, err
	}

	return Entry{Value: value}, nil
}

// decode uses specified types from structure's schema to cast csv string values to their
// intended types. If casting fails because the data is invalid, it's left as a string instead
// of causing an error.
func (r *CSVReader) decode(strings []string) ([]interface{}, error) {
	vs := make([]interface{}, len(strings))
	types := r.types
	if len(types) < len(strings) {
		// TODO - fix. for now is types fails to parse we just assume all types
		// are strings
		types = make([]string, len(strings))
		for i := range types {
			types[i] = "string"
		}
	}
	for i, str := range strings {
		vs[i] = str

		switch types[i] {
		case "number":
			if num, err := vals.ParseNumber([]byte(str)); err == nil {
				vs[i] = num
			}
		case "integer":
			if num, err := vals.ParseInteger([]byte(str)); err == nil {
				vs[i] = num
			}
		case "boolean":
			if b, err := vals.ParseBoolean([]byte(str)); err == nil {
				vs[i] = b
			}
		case "object":
			v := map[string]interface{}{}
			if err := json.Unmarshal([]byte(str), &v); err == nil {
				vs[i] = v
			}
		case "array":
			v := []interface{}{}
			if err := json.Unmarshal([]byte(str), &v); err == nil {
				vs[i] = v
			}
		case "null":
			vs[i] = nil
		}
	}

	return vs, nil
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
	types       []string
}

// NewCSVWriter creates a Writer from a structure and write destination
func NewCSVWriter(st *dataset.Structure, w io.Writer) *CSVWriter {
	// TODO - capture error
	titles, types, _ := terribleHackToGetHeaderRowAndTypes(st)

	writer := csv.NewWriter(w)
	wr := &CSVWriter{
		st:    st,
		w:     writer,
		types: types,
	}

	if CSVOpts, ok := st.FormatConfig.(*dataset.CSVOptions); ok {
		if CSVOpts.HeaderRow {
			writer.Write(titles)
		}
	}

	return wr
}

// TODO - holy shit dis so bad. fix
func terribleHackToGetHeaderRowAndTypes(st *dataset.Structure) ([]string, []string, error) {
	data, err := st.Schema.MarshalJSON()
	if err != nil {
		return nil, nil, err
	}
	sch := map[string]interface{}{}
	if err := json.Unmarshal(data, &sch); err != nil {
		return nil, nil, err
	}
	if itemObj, ok := sch["items"].(map[string]interface{}); ok {
		if itemArr, ok := itemObj["items"].([]interface{}); ok {
			titles := make([]string, len(itemArr))
			types := make([]string, len(itemArr))
			for i, f := range itemArr {
				if field, ok := f.(map[string]interface{}); ok {
					if title, ok := field["title"].(string); ok {
						titles[i] = title
					}

					if ts, ok := field["type"].(string); ok {
						types[i] = ts
					} else if ta, ok := field["type"].([]interface{}); ok && len(ta) > 0 {
						if st, ok := ta[0].(string); ok {
							types[i] = st
						} else {
							types[i] = "string"
						}
					} else {
						types[i] = "string"
					}
				}
			}
			return titles, types, nil
		}
	}
	return nil, nil, fmt.Errorf("nope")
}

// Structure gives this writer's structure
func (w *CSVWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteEntry writes one CSV record to the writer
func (w *CSVWriter) WriteEntry(ent Entry) error {
	if arr, ok := ent.Value.([]interface{}); ok {
		strs, err := encode(arr)
		if err != nil {
			return fmt.Errorf("error encoding entry: %s", err.Error())
		}
		return w.w.Write(strs)
	}
	return fmt.Errorf("expected array value to write csv row. got: %v", ent)
}

// encode uses specified types from structure's schema to go values to strings
func encode(vs []interface{}) ([]string, error) {
	strings := make([]string, len(vs))

	for i, v := range vs {
		// vs[i] = str
		switch t := v.(type) {
		case string:
			strings[i] = t
		case int:
			strings[i] = strconv.Itoa(t)
		case int64:
			strings[i] = strconv.Itoa(int(t))
		case float64:
			strings[i] = strconv.FormatFloat(t, 'f', -1, 64)
		case []interface{}:
			if data, err := json.Marshal(t); err == nil {
				strings[i] = string(data)
			}
		case map[string]interface{}:
			if data, err := json.Marshal(t); err == nil {
				strings[i] = string(data)
			}
		case bool:
			if t {
				strings[i] = "true"
			} else {
				strings[i] = "false"
			}
		case nil:
			strings[i] = ""
		}
	}

	return strings, nil
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *CSVWriter) Close() error {
	w.w.Flush()
	return nil
}

// ReplaceSoloCarriageReturns wraps an io.Reader, on every call of Read. it looks for
// for instances of lonely \r replacing them with \r\n before returning to the end consumer
// lots of files in the wild will come without "proper" line breaks, which irritates go's
// standard csv package. This'll fix by wrapping the reader passed to csv.NewReader:
// 		rdr, err := csv.NewReader(ReplaceSoloCarriageReturns(r))
//
func ReplaceSoloCarriageReturns(data io.Reader) io.Reader {
	return crlfReplaceReader{
		rdr: bufio.NewReader(data),
	}
}

// crlfReplaceReader wraps a reader
type crlfReplaceReader struct {
	rdr *bufio.Reader
}

// Read implements io.Reader for crlfReplaceReader
func (c crlfReplaceReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}

	for {
		if n == len(p) {
			return
		}

		p[n], err = c.rdr.ReadByte()
		if err != nil {
			return
		}

		// any time we encounter \r & still have space, check to see if \n follows
		// ff next char is not \n, add it in manually
		if p[n] == '\r' && n < len(p) {
			if pk, err := c.rdr.Peek(1); (err == nil && pk[0] != '\n') || (err != nil && err.Error() == "EOF") {
				n++
				p[n] = '\n'
			}
		}

		n++
	}
	return
}
