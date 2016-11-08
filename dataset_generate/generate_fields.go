package dataset_generate

import (
	"math/rand"

	"github.com/qri-io/dataset"
	"github.com/qri-io/datatype"
	"github.com/qri-io/datatype/datatype_generate"
)

// GenerateRandomFieldsOpt specifies the options for GenerateRandomFields
type RandomFieldsOpt struct {
	// use a provided name instead of a random one
	Name string
	// number of random fields to generate, default between 1 & 10
	NumFields int
	// constrict creation to provided types, blank means any valid datatype
	Datatypes []datatype.Type
	// set fields to get a specific set of fields back
	// overrides numfields, datatypes
	Fields []*dataset.Field
}

// GenerateRandomFields creates a random valid table. Provide option func(s) to customize
func RandomFields(options ...func(*RandomFieldsOpt)) []*dataset.Field {
	opt := &RandomFieldsOpt{
		Name:      randString(16),
		NumFields: rand.Intn(9) + 1,
		Fields:    nil,
	}
	for _, option := range options {
		option(opt)
	}

	if opt.Fields == nil {
		opt.Fields = make([]*dataset.Field, opt.NumFields)
		for i := 0; i < opt.NumFields; i++ {
			opt.Fields[i] = RandomField(func(o *RandomFieldOpt) {
				o.Datatypes = opt.Datatypes
			})
		}
	}

	return opt.Fields
}

// RandomFieldOpt are the options for RandomField
type RandomFieldOpt struct {
	// use a provided name instead of a random one
	Name string
	// use a provided type instead of a random one
	Type datatype.Type
	// constrict random types to a provided set, blank means any valid datatype
	Datatypes []datatype.Type
}

// RandomField generates a random field, optionally configured
func RandomField(options ...func(*RandomFieldOpt)) *dataset.Field {
	opt := &RandomFieldOpt{
		Name:      randString(16),
		Datatypes: nil,
	}
	for _, option := range options {
		option(opt)
	}

	if opt.Type == datatype.Unknown {
		if opt.Datatypes != nil {
			opt.Type = opt.Datatypes[rand.Intn((len(opt.Datatypes)-1))+1]
		} else {
			opt.Type = datatype.Type(rand.Intn(datatype.NUM_DATA_TYPES) + 1)
		}
	}

	return &dataset.Field{
		Name: opt.Name,
		Type: opt.Type,
	}
}

// Random Rows generates random row data
func RandomRows(fields []*dataset.Field, numRows int) (rows [][]interface{}) {
	rows = make([][]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		row := make([]interface{}, len(fields))
		for j, field := range fields {
			row[j] = datatype_generate.RandomValue(field.Type)
		}
		rows[i] = row
	}

	return
}
