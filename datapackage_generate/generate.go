package datapackage_generate

import (
	"math/rand"
	"time"

	"github.com/qri-io/datapackage"
	"github.com/qri-io/datatype"
	"github.com/qri-io/jsontable"
	"github.com/qri-io/jsontable/jsontable_generate"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RandomPackageOpts struct {
	Name         datapackage.Name
	Title        string
	NumResources int
	Resources    []*datapackage.Resource
	Datatypes    []datatype.Type
}

func RandomPackage(options ...func(*RandomPackageOpts)) *datapackage.Package {
	opt := &RandomPackageOpts{
		Name:         RandomName(16),
		Title:        randString(80),
		NumResources: rand.Intn(10) + 1,
	}
	for _, option := range options {
		option(opt)
	}

	if opt.Resources == nil && opt.NumResources > 0 {
		opt.Resources = make([]*datapackage.Resource, opt.NumResources)
		for i := 0; i < opt.NumResources; i++ {
			opt.Resources[i] = RandomResource(func(o *RandomResourceOpts) {
				o.Datatypes = opt.Datatypes
			})
		}
	}

	return &datapackage.Package{
		Name:      opt.Name,
		Title:     opt.Title,
		Resources: opt.Resources,
	}
}

type RandomResourceOpts struct {
	Name      datapackage.Name
	NumFields int
	Datatypes []datatype.Type
	Schema    *jsontable.Table
}

func RandomResource(options ...func(*RandomResourceOpts)) *datapackage.Resource {
	opt := &RandomResourceOpts{
		Name:      RandomName(16),
		NumFields: rand.Intn(9) + 1,
		Datatypes: nil,
	}

	for _, option := range options {
		option(opt)
	}

	if opt.Schema == nil && opt.NumFields > 0 {
		opt.Schema = jsontable_generate.RandomTable(func(o *jsontable_generate.RandomTableOpt) {
			o.NumFields = opt.NumFields
			o.Datatypes = opt.Datatypes
		})
	}

	return &datapackage.Resource{
		Name:   opt.Name,
		Schema: opt.Schema,
	}
}

func RandomName(maxLength int) datapackage.Name {
	return datapackage.Name(randString(rand.Intn(maxLength-1) + 1))
}

var alphaNumericRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = alphaNumericRunes[rand.Intn(len(alphaNumericRunes))]
	}
	return string(b)
}
