// Package generate is for generating random data from given structures
package generate

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

// Generator is a dsio.EntryReader that creates a new entry on each call to ReadEntry
type Generator struct {
	// structure will hold the jsonschema that generator should use
	structure *dataset.Structure
	// random number generator, per-instance for testing
	random *rand.Rand
	// maximum length of random values to generate
	maxLen int
	// when generating array entries
	count int
	// only two possible structures for now are "array" or "object"
	schemaIsArray bool
	// whether to produce random types of values, or always use strings
	useRandomType bool
}

// ReadEntry implements the dsio.EntryReader interface
func (g *Generator) ReadEntry() (dsio.Entry, error) {
	var value interface{}
	if g.useRandomType {
		// Produce different types of values, using completely arbitrary odds.
		typeChoice := g.random.Intn(64)
		if typeChoice == 0 {
			value = nil
		} else if typeChoice == 1 {
			value = false
		} else if typeChoice == 2 {
			value = true
		} else if typeChoice < 24 {
			value = g.randString()
		} else if typeChoice < 44 {
			value = g.random.Float64()
		} else {
			value = g.random.Int()
		}
	} else {
		value = g.randString()
	}
	// TODO: Actually inspect the structure more deeply than simply "array" vs "object".
	if g.schemaIsArray {
		index := g.count
		g.count++
		return dsio.Entry{Index: index, Value: value}, nil
	}
	return dsio.Entry{Key: g.randString(), Value: value}, nil
}

// Structure implements the dsio.EntryReader interface
func (g Generator) Structure() *dataset.Structure {
	return g.structure
}

// Close finalizes the generator
func (g Generator) Close() error {
	return nil
}

var alphaNumericRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// randString generates a random string of alpha numeric characters up to maxLen runes long.
func (g Generator) randString() string {
	n := g.random.Intn(g.maxLen)
	bytes := make([]rune, n)
	for i := range bytes {
		bytes[i] = alphaNumericRunes[g.random.Intn(len(alphaNumericRunes))]
	}
	return string(bytes)
}

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

// NewGenerator creates a generator with the given configuration options
func NewGenerator(st *dataset.Structure, options ...func(*Config)) (*Generator, error) {
	cfg := DefaultConfig()
	for _, opt := range options {
		opt(cfg)
	}

	// Convert the schema to a string, check for "array" string in the result.
	// TODO (dlong): Inspect the structure more deeply than simply "array" vs "object".
	if st.Schema == nil {
		return nil, fmt.Errorf("structure.Schema is required")
	}
	tlt, ok := st.Schema["type"].(string)
	if !ok {
		return nil, fmt.Errorf("structure.Schema top level type must be a string")
	}
	schemaIsArray := tlt == "array"
	return &Generator{
		structure:     st,
		maxLen:        cfg.maxLen,
		random:        cfg.random,
		schemaIsArray: schemaIsArray,
		useRandomType: false}, nil
}
