package generate

import (
	"github.com/ipfs/go-datastore"
	"math/rand"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/datatypes"
)

type RandomStructureOpts struct {
	Path      datastore.Key
	Datatypes []datatypes.Type
	Format    dataset.DataFormat
	NumFields int
	Fields    []*dataset.Field
	// Data           []byte
	// NumRandRecords int
}

// RandomStructure generates a randomized resource definition
func RandomStructure(options ...func(*RandomStructureOpts)) *dataset.Structure {
	opt := &RandomStructureOpts{
		NumFields: rand.Intn(9) + 1,
		Datatypes: nil,
		Format:    dataset.CSVDataFormat,
	}

	for _, option := range options {
		option(opt)
	}

	if opt.Fields == nil && opt.NumFields > 0 {
		opt.Fields = RandomFields(func(o *RandomFieldsOpt) {
			o.NumFields = opt.NumFields
			o.Datatypes = opt.Datatypes
		})
	}

	ds := &dataset.Structure{
		Format: opt.Format,
		Schema: &dataset.Schema{
			Fields: opt.Fields,
		},
		// Data: opt.Data,
	}

	return ds
}
