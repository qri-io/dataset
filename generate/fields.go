package generate

import (
	"math/rand"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/datatypes"
	// "github.com/qri-io/datatypes/datatypes_generate"
)

// GenerateRandomFieldsOpt specifies the options for GenerateRandomFields
type RandomFieldsOpt struct {
	// use a provided name instead of a random one
	Name string
	// number of random fields to generate, default between 1 & 10
	NumFields int
	// constrict creation to provided types, blank means any valid datatypes
	Datatypes []datatypes.Type
	// set fields to get a specific set of fields back
	// overrides numfields, datatypess
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
	Type datatypes.Type
	// constrict random types to a provided set, blank means any valid datatypes
	Datatypes []datatypes.Type
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

	if opt.Type == datatypes.Unknown {
		if opt.Datatypes != nil {
			opt.Type = opt.Datatypes[rand.Intn((len(opt.Datatypes)-1))+1]
		} else {
			opt.Type = datatypes.Type(rand.Intn(datatypes.NUM_DATA_TYPES) + 1)
		}
	}

	return &dataset.Field{
		Name: opt.Name,
		Type: opt.Type,
	}
}
