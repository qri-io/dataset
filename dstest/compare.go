package dstest

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/qri-io/dataset"
)

// CompareDatasets checks two given datasets for equality, returng a diff string
// describing the difference between each dataset/ return will be the empty
// string
// if datasets are equal
// CompareDatasets defaults to a strict compraison of all exported fields
// operates on copies of passed-in datasets to keep this function free of side
// effects
func CompareDatasets(expect, got *dataset.Dataset, opts ...CompareDatasetsOpt) string {
	cfg := &CompareDatasetConfig{}
	for _, opt := range opts {
		opt.Apply(cfg)
	}

	a := &dataset.Dataset{}
	a.Assign(expect)

	b := &dataset.Dataset{}
	b.Assign(got)

	if cfg.dropTransients {
		a.DropTransientValues()
		a.DropTransientValues()
	}

	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(
		dataset.Dataset{},
		dataset.Meta{},
		dataset.Transform{},
		dataset.Readme{},
		dataset.Viz{},
	))
}

// CompareDatasetConfig defines unexported configuration parameters, which are
// set via CompareDatasetOpt's
type CompareDatasetConfig struct {
	dropTransients bool
}

// CompareDatasetsOpt adusts the CompareDatasets function
type CompareDatasetsOpt interface {
	Apply(cfg *CompareDatasetConfig)
}

// OptDropTransientValues drops transients on both dataset before making the
// comparison, allowing things like dataset name &
type OptDropTransientValues int

// Apply sets unexported configuration
func (OptDropTransientValues) Apply(cfg *CompareDatasetConfig) {
	cfg.dropTransients = true
}
