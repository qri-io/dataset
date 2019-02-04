package dsio

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/vals"
)

// XLSXReader implements the RowReader interface for the XLSX data format
type XLSXReader struct {
	err       error
	st        *dataset.Structure
	sheetName string
	file      *excelize.File
	r         *excelize.Rows
	idx       int
	types     []string
}

// NewXLSXReader creates a reader from a structure and read source
func NewXLSXReader(st *dataset.Structure, r io.Reader) (*XLSXReader, error) {
	// TODO - handle error
	_, types, _ := terribleHackToGetHeaderRowAndTypes(st)

	rdr := &XLSXReader{
		st:    st,
		types: types,
	}

	// xlsxr := xlsx.NewReader(ReplaceSoloCarriageReturns(r))
	rdr.file, rdr.err = excelize.OpenReader(r)
	if rdr.err != nil {
		return rdr, rdr.err
	}

	if fcg, err := dataset.ParseFormatConfigMap(dataset.XLSXDataFormat, st.FormatConfig); err == nil {
		if opts, ok := fcg.(*dataset.XLSXOptions); ok {
			rdr.sheetName = opts.SheetName
		}
	}
	if rdr.sheetName == "" {
		rdr.sheetName = "Sheet1"
	}

	if rdr.err == nil {
		rdr.r, rdr.err = rdr.file.Rows(rdr.sheetName)
	}

	return rdr, nil
}

// Structure gives this reader's structure
func (r *XLSXReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one XLSX record from the reader
func (r *XLSXReader) ReadEntry() (Entry, error) {
	if r.err != nil {
		return Entry{}, r.err
	}
	if !r.r.Next() {
		return Entry{}, io.EOF
	}
	vals, err := r.decode(r.r.Columns())
	if err != nil {
		return Entry{}, err
	}
	ent := Entry{Index: r.idx, Value: vals}
	r.idx++

	return ent, nil
}

// decode uses specified types from structure's schema to cast xlsx string values to their
// intended types. If casting fails because the data is invalid, it's left as a string instead
// of causing an error.
func (r *XLSXReader) decode(strings []string) ([]interface{}, error) {
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

// Close finalizes the writer, indicating no more records will be read
func (r *XLSXReader) Close() error {
	return nil
}

// XLSXWriter implements the RowWriter interface for
// XLSX-formatted data
type XLSXWriter struct {
	rowsWritten int
	sheetName   string
	f           *excelize.File
	st          *dataset.Structure
	w           io.Writer
	types       []string
}

// NewXLSXWriter creates a Writer from a structure and write destination
func NewXLSXWriter(st *dataset.Structure, w io.Writer) (*XLSXWriter, error) {
	// TODO - capture error
	_, types, _ := terribleHackToGetHeaderRowAndTypes(st)

	wr := &XLSXWriter{
		st:    st,
		f:     excelize.NewFile(),
		types: types,
		w:     w,
	}

	if fcg, err := dataset.ParseFormatConfigMap(dataset.XLSXDataFormat, st.FormatConfig); err == nil {
		if opts, ok := fcg.(*dataset.XLSXOptions); ok {
			wr.sheetName = opts.SheetName
		}
	} else {
		return nil, err
	}

	if wr.sheetName == "" {
		wr.sheetName = "Sheet1"
	}

	idx := wr.f.NewSheet(wr.sheetName)
	wr.f.SetActiveSheet(idx)

	return wr, nil
}

// Structure gives this writer's structure
func (w *XLSXWriter) Structure() *dataset.Structure {
	return w.st
}

// WriteEntry writes one XLSX record to the writer
func (w *XLSXWriter) WriteEntry(ent Entry) error {
	if arr, ok := ent.Value.([]interface{}); ok {
		strs, err := encodeStrings(arr)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error encoding entry: %s", err.Error())
		}
		for i, str := range strs {
			w.f.SetCellValue(w.sheetName, w.axis(i), str)
		}
		w.rowsWritten++
		return nil
	}
	return fmt.Errorf("expected array value to write xlsx row. got: %v", ent)
}

func (w *XLSXWriter) axis(colIDx int) string {
	return ColIndexToLetters(colIDx) + strconv.Itoa(w.rowsWritten+1)
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *XLSXWriter) Close() error {
	_, err := w.f.WriteTo(w.w)
	return err
}

func encodeStrings(vs []interface{}) (strs []string, err error) {
	strs = make([]string, len(vs))
	for i, v := range vs {
		if v == nil {
			continue
		}
		switch x := v.(type) {
		case int:
			strs[i] = strconv.Itoa(x)
		case int64:
			strs[i] = strconv.Itoa(int(x))
		case float64:
			strs[i] = strconv.FormatFloat(x, 'f', -1, 64)
		case bool:
			strs[i] = strconv.FormatBool(x)
		case string:
			strs[i] = x
		case []interface{}:
			data, err := json.Marshal(x)
			if err != nil {
				return strs, err
			}
			strs[i] = string(data)
		case map[string]interface{}:
			data, err := json.Marshal(x)
			if err != nil {
				return strs, err
			}
			strs[i] = string(data)
		default:
			return strs, fmt.Errorf("unrecognized encoding type: %#v", v)
		}
	}
	return
}

// ColIndexToLetters is used to convert a zero based, numeric column
// indentifier into a character code.
func ColIndexToLetters(colRef int) string {
	parts := intToBase26(colRef)
	return formatColumnName(smooshBase26Slice(parts))
}

// Get the largestDenominator that is a multiple of a basedDenominator
// and fits at least once into a given numerator.
func getLargestDenominator(numerator, multiple, baseDenominator, power int) (int, int) {
	if numerator/multiple == 0 {
		return 1, power
	}
	next, nextPower := getLargestDenominator(
		numerator, multiple*baseDenominator, baseDenominator, power+1)
	if next > multiple {
		return next, nextPower
	}
	return multiple, power
}

// Converts a list of numbers representing a column into a alphabetic
// representation, as used in the spreadsheet.
func formatColumnName(colID []int) string {
	lastPart := len(colID) - 1

	result := ""
	for n, part := range colID {
		if n == lastPart {
			// The least significant number is in the
			// range 0-25, all other numbers are 1-26,
			// hence we use a differente offset for the
			// last part.
			result += string(part + 65)
		} else {
			// Don't output leading 0s, as there is no
			// representation of 0 in this format.
			if part > 0 {
				result += string(part + 64)
			}
		}
	}
	return result
}

func smooshBase26Slice(b26 []int) []int {
	// Smoosh values together, eliminating 0s from all but the
	// least significant part.
	lastButOnePart := len(b26) - 2
	for i := lastButOnePart; i > 0; i-- {
		part := b26[i]
		if part == 0 {
			greaterPart := b26[i-1]
			if greaterPart > 0 {
				b26[i-1] = greaterPart - 1
				b26[i] = 26
			}
		}
	}
	return b26
}

func intToBase26(x int) (parts []int) {
	// Excel column codes are pure evil - in essence they're just
	// base26, but they don't represent the number 0.
	b26Denominator, _ := getLargestDenominator(x, 1, 26, 0)

	// This loop terminates because integer division of 1 / 26
	// returns 0.
	for d := b26Denominator; d > 0; d = d / 26 {
		value := x / d
		remainder := x % d
		parts = append(parts, value)
		x = remainder
	}
	return parts
}
