package dataset_generate

import (
	"bytes"
	"encoding/csv"
	"math/rand"
	"time"

	"github.com/qri-io/dataset"
	"github.com/qri-io/datatype"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RandomDatasetOpts struct {
	Name           string
	Address        dataset.Address
	Title          string
	NumDatasets    int
	Datasets       []*dataset.Dataset
	Datatypes      []datatype.Type
	Format         dataset.DataFormat
	NumFields      int
	Fields         []*dataset.Field
	Data           []byte
	NumRandRecords int
}

func RandomDataset(options ...func(*RandomDatasetOpts)) *dataset.Dataset {
	name := randString(16)
	opt := &RandomDatasetOpts{
		Name:           name,
		Address:        dataset.NewAddress(name),
		NumFields:      rand.Intn(9) + 1,
		Datatypes:      nil,
		NumRandRecords: 0,
		Format:         dataset.CsvDataFormat,
	}

	for _, option := range options {
		option(opt)
	}

	if opt.Datasets == nil && opt.NumDatasets > 0 {
		opt.Datasets = make([]*dataset.Dataset, opt.NumDatasets)
		for i := 0; i < opt.NumDatasets; i++ {
			opt.Datasets[i] = RandomDataset(func(o *RandomDatasetOpts) {
				o.Datatypes = opt.Datatypes
			})
		}
	}

	if opt.Fields == nil && opt.NumFields > 0 {
		opt.Fields = RandomFields(func(o *RandomFieldsOpt) {
			o.NumFields = opt.NumFields
			o.Datatypes = opt.Datatypes
		})
	}

	ds := &dataset.Dataset{
		Name:     opt.Name,
		Address:  opt.Address,
		Datasets: opt.Datasets,
		Format:   opt.Format,
		Fields:   opt.Fields,
		Data:     opt.Data,
	}

	if opt.NumRandRecords > 0 && opt.Format == dataset.CsvDataFormat {
		buf := bytes.NewBuffer(ds.Data)
		if err := csv.NewWriter(buf).WriteAll(RandomStringRows(ds.Fields, opt.NumRandRecords)); err != nil {
			panic(err)
		}
		ds.Data = buf.Bytes()
	}

	return ds
}

var alphaNumericRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var alphaRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = alphaNumericRunes[rand.Intn(len(alphaNumericRunes))]
	}
	return string(b)
}
