package generate

import (
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"math/rand"
	"testing"
)

// Compile time check that Generator satisfies the EntryReader interace.
var _ dsio.EntryReader = (*Generator)(nil)

func AssignSeed(cfg *Config) {
	cfg.random = rand.New(rand.NewSource(4))
}

func AssignMaxLen(cfg *Config) {
	cfg.maxLen = 8
}

func TestGeneratorForBaseSchemaArray(t *testing.T) {
	sta := dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaArray}
	g, _ := NewGenerator(&sta, AssignSeed, AssignMaxLen)
	cases := []struct {
		index int
		key   string
		value string
	}{
		{0, "", "p"},
		{1, "", "nfgDsc2"},
		{2, "", "D8F2qN"},
	}

	for i, c := range cases {
		e, _ := g.ReadEntry()
		if e.Index != c.index {
			t.Errorf("case %d index mismatch. expected: %d. got: %d", i, c.index, e.Index)
		}
		if e.Key != c.key {
			t.Errorf("case %d key mismatch. expected: %s. got: %s", i, c.key, e.Key)
		}
		if e.Value != c.value {
			t.Errorf("case %d value mismatch. expected: %s. got: %s", i, c.value, e.Value)
		}
	}
}

func TestGeneratorForBaseSchemaObject(t *testing.T) {
	sta := dataset.Structure{Format: dataset.JSONDataFormat, Schema: dataset.BaseSchemaObject}
	g, _ := NewGenerator(&sta, AssignSeed, AssignMaxLen)
	cases := []struct {
		index int
		key   string
		value string
	}{
		{0, "HK5a8", "jj"},
		{0, "kwzDkh9", "2fhfU"},
		{0, "uS9jZ", "uVbhV3"},
	}

	for i, c := range cases {
		e, _ := g.ReadEntry()
		if e.Index != c.index {
			t.Errorf("case %d index mismatch. expected: %d. got: %d", i, e.Index, c.index)
		}
		if e.Key != c.key {
			t.Errorf("case %d key mismatch. expected: %s. got: %s", i, e.Key, c.key)
		}
		if e.Value != c.value {
			t.Errorf("case %d value mismatch. expected: %s. got: %s", i, e.Value, c.value)
		}
	}
}
