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
func CompareDatasets(expect, got *dataset.Dataset, opts ...CompareOpts) string {
	cfg := &CompareConfig{}
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
		dataset.Commit{},
		dataset.Meta{},
		dataset.Transform{},
		dataset.Readme{},
		dataset.Viz{},
	))
}

// CompareConfig defines configuration parameters, which are unexported, but
// settable via CompareOpt's supplied ot a Compare function
type CompareConfig struct {
	dropTransients bool
}

// CompareOpts adusts component comparison functions
type CompareOpts interface {
	Apply(cfg *CompareConfig)
}

// OptDropTransientValues drops transients on both dataset before making the
// comparison, allowing things like dataset name &
type OptDropTransientValues int

// Apply sets unexported configuration
func (OptDropTransientValues) Apply(cfg *CompareConfig) {
	cfg.dropTransients = true
}

// CompareCommits is CompareDatasets, but for commit components
func CompareCommits(expect, got *dataset.Commit, opts ...CompareOpts) string {
	cfg := &CompareConfig{}
	for _, opt := range opts {
		opt.Apply(cfg)
	}

	a := &dataset.Commit{}
	a.Assign(expect)

	b := &dataset.Commit{}
	b.Assign(got)

	if cfg.dropTransients {
		a.DropTransientValues()
		a.DropTransientValues()
	}

	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(
		dataset.Commit{},
	))
}

// CompareMetas is CompareDatasets, but for meta components
func CompareMetas(expect, got *dataset.Meta, opts ...CompareOpts) string {
	cfg := &CompareConfig{}
	for _, opt := range opts {
		opt.Apply(cfg)
	}

	a := &dataset.Meta{}
	a.Assign(expect)

	b := &dataset.Meta{}
	b.Assign(got)

	if cfg.dropTransients {
		a.DropTransientValues()
		a.DropTransientValues()
	}

	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(
		dataset.Meta{},
	))
}

// CompareStructures is CompareDatasets, but for structure components
func CompareStructures(expect, got *dataset.Structure, opts ...CompareOpts) string {
	cfg := &CompareConfig{}
	for _, opt := range opts {
		opt.Apply(cfg)
	}

	a := &dataset.Structure{}
	a.Assign(expect)

	b := &dataset.Structure{}
	b.Assign(got)

	if cfg.dropTransients {
		a.DropTransientValues()
		a.DropTransientValues()
	}

	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(
		dataset.Structure{},
	))
}
