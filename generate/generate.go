// Package generate is for generating random data from given structures
package generate

import (
	"fmt"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"math/rand"
	"strings"
	"time"
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
}

// ReadEntry implements the dsio.EntryReader interface
func (g *Generator) ReadEntry() (dsio.Entry, error) {
	// TODO: Actually inspect the structure more deeply than simply "array" vs "object".
	if g.schemaIsArray {
		index := g.count
		g.count++
		return dsio.Entry{Index: index, Value: g.randString()}, nil
	}
	return dsio.Entry{Key: g.randString(), Value: g.randString()}, nil
}

// Structure implements the dsio.EntryReader interface
func (g Generator) Structure() *dataset.Structure {
	return g.structure
}

var alphaNumericRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// randString generates a random string of alpha numeric characters up to maxLen runes long.
func (g Generator) randString() string {
	n := rand.Intn(g.maxLen)
	bytes := make([]rune, n)
	for i := range bytes {
		bytes[i] = alphaNumericRunes[rand.Intn(len(alphaNumericRunes))]
	}
	return string(bytes)
}

// Config stores settings for the generate package.
type Config struct {
	random *rand.Rand
	maxLen int
}

// DefaultConfig returns the default configuration for a Generator.
func DefaultConfig() *Config {
	return &Config{
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLen: 64,
	}
}

// NewGenerator creates a generator with the given configuration options
func NewGenerator(st *dataset.Structure, options ...func(*Config)) (*Generator, error) {
	cfg := DefaultConfig()
	for _, opt := range options {
		opt(cfg)
	}

	// Convert the schema to a string, check for "array" string in the result.
	// TODO: Inspect the structure more deeply than simply "array" vs "object".
	pather := fmt.Sprintf("%s", st.Schema.Schema.JSONChildren()["type"])
	schemaIsArray := false
	if strings.Contains(pather, "array") {
		schemaIsArray = true
	}
	return &Generator{
		structure:     st,
		maxLen:        cfg.maxLen,
		random:        cfg.random,
		schemaIsArray: schemaIsArray}, nil
}
