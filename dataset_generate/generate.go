package dataset_generate

import (
	"math/rand"
	"time"

	"github.com/qri-io/dataset"
	"github.com/qri-io/datatype"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RandomDatasetOpts struct {
	Name        dataset.Name
	Title       string
	NumDatasets int
	Datasets    []*dataset.Dataset
	Datatypes   []datatype.Type
	NumFields   int
	Fields      []*dataset.Field
}

func RandomDataset(options ...func(*RandomDatasetOpts)) *dataset.Dataset {
	opt := &RandomDatasetOpts{
		Name:      RandomName(16),
		NumFields: rand.Intn(9) + 1,
		Datatypes: nil,
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
		Datasets: opt.Datasets,
	}

	ds.Fields = opt.Fields

	return ds
}

func RandomName(maxLength int) dataset.Name {
	return dataset.Name(randString(rand.Intn(maxLength-1) + 1))
}

var alphaNumericRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = alphaNumericRunes[rand.Intn(len(alphaNumericRunes))]
	}
	return string(b)
}
