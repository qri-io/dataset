package validate

import (
	"github.com/qri-io/dataset"

	//"fmt"
	//"io"
	"strings"
	"testing"
)

//Note text examples in testdata.go

func TestDataFormat(t *testing.T) {
	cases := []struct {
		df    dataset.DataFormat
		input string
		err   string
	}{
		{dataset.JsonDataFormat,
			rawText1,
			"error: data format 'JsonData' not currently supported",
		},
		{dataset.JsonArrayDataFormat,
			rawText1,
			"error: data format 'JsonArrayData' not currently supported",
		},
		{
			dataset.XlsDataFormat,
			rawText1,
			"error: data format 'XlsData' not currently supported",
		},
		{
			dataset.XmlDataFormat,
			rawText1,
			"error: data format 'XmlData' not currently supported",
		},
		{
			dataset.UnknownDataFormat,
			rawText1,
			"error: unknown data format not currently supported",
		},
		{
			dataset.DataFormat(999),
			rawText1,
			"error: data format not currently supported",
		},
		{
			dataset.CsvDataFormat,
			rawText4,
			"error: inconsistent column length on line 4 of length 2 (rather than 1). ensure all csv columns same length",
		},
		{
			dataset.CsvDataFormat,
			rawText1,
			"",
		},
	}
	for i, c := range cases {
		r := strings.NewReader(c.input)
		err := DataFormat(c.df, r)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case [%d] error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}

}
