package generate

import (
	"bytes"
	"encoding/csv"
	"github.com/qri-io/dataset"
)

// RandomDataOpts configures RandomData output
type RandomDataOpts struct {
	Data           []byte
	NumRandRecords int
	Format         dataset.DataFormat
}

// RandomData generates data based on a given resource definition
func RandomData(st *dataset.Structure, opts ...func(o *RandomDataOpts)) []byte {
	opt := &RandomDataOpts{
		Data:           nil,
		NumRandRecords: 500,
		Format:         dataset.CSVDataFormat,
	}
	for _, option := range opts {
		option(opt)
	}
	if opt.NumRandRecords == 0 || opt.Format != dataset.CSVDataFormat {
		return nil
	}

	buf := bytes.NewBuffer(opt.Data)
	if err := csv.NewWriter(buf).WriteAll(RandomStringRows(st.Schema.Fields, opt.NumRandRecords)); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

// RandomRows generates random row data
func RandomRows(fields []*dataset.Field, numRows int) (rows [][]interface{}) {
	rows = make([][]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		row := make([]interface{}, len(fields))
		for j, field := range fields {
			row[j] = RandomValue(field.Type)
		}
		rows[i] = row
	}
	return
}

// RandomStringRows generates random row data as strings
func RandomStringRows(fields []*dataset.Field, numRows int) (rows [][]string) {
	rows = make([][]string, numRows)
	for i := 0; i < numRows; i++ {
		row := make([]string, len(fields))
		for j, field := range fields {
			row[j] = RandomStringValue(field.Type)
		}
		rows[i] = row
	}
	return
}
