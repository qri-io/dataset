// Package generate is for generating random data from given structures
package generate

import (
	"math/rand"
	"time"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/tabular"
)

// Config stores settings for the generate package.
type Config struct {
	random        *rand.Rand
	maxLen        int
	useRandomType bool
}

// DefaultConfig returns the default configuration for a Generator.
func DefaultConfig() *Config {
	return &Config{
		random:        rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLen:        64,
		useRandomType: false,
	}
}

// AssignSeed sets a specific random seed to be used.
func AssignSeed(cfg *Config) {
	cfg.random = rand.New(rand.NewSource(4))
}

// AssignMaxLen sets a maximum length for generated values.
func AssignMaxLen(cfg *Config) {
	cfg.maxLen = 8
}

// AssignUseRandomType causes generator to generate random types of values.
func AssignUseRandomType(cfg *Config) {
	cfg.useRandomType = true
}

// TabularGenerator is a dsio.EntryReader that creates a new entry on each call
// to ReadEntry
type TabularGenerator struct {
	cols      tabular.Columns
	structure *dataset.Structure
	gen       *ValueGenerator
	// when generating array entries
	count int
	// only two possible structures for now are "array" or "object"
	schemaIsArray bool
}

// assert at compile time that Generator is a dsio.EntryReader
var _ dsio.EntryReader = (*TabularGenerator)(nil)

// NewTabularGenerator creates a tablular data generator with the given
// configuration options
func NewTabularGenerator(st *dataset.Structure, options ...func(*Config)) (*TabularGenerator, error) {
	cfg := DefaultConfig()
	for _, opt := range options {
		opt(cfg)
	}

	cols, _, err := tabular.ColumnsFromJSONSchema(st.Schema)
	if err != nil {
		return nil, err
	}

	gen := &ValueGenerator{
		Rand:            cfg.random,
		MaxStringLength: cfg.maxLen,
	}

	return &TabularGenerator{
		structure:     st,
		cols:          cols,
		gen:           gen,
		schemaIsArray: true,
	}, nil
}

// ReadEntry implements the dsio.EntryReader interface
func (g *TabularGenerator) ReadEntry() (dsio.Entry, error) {
	row := make([]interface{}, len(g.cols))
	for i, col := range g.cols {
		row[i] = g.gen.Type([]string(*col.Type)[0])
	}
	index := g.count
	g.count++
	return dsio.Entry{Index: index, Value: row}, nil
}

// Structure implements the dsio.EntryReader interface
func (g TabularGenerator) Structure() *dataset.Structure {
	return g.structure
}

// Close finalizes the generator
func (g TabularGenerator) Close() error {
	return nil
}
