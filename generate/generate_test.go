package generate

import (
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

// Compile time check that Generator satisfies the EntryReader interace.
var _ dsio.EntryReader = (*Generator)(nil)

func TestGeneratorForBaseSchemaArray(t *testing.T) {
	sta := dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray}
	g, _ := NewGenerator(&sta, AssignSeed, AssignMaxLen)
	cases := []struct {
		index int
		key   string
		value string
	}{
		{0, "", "gltBH"},
		{1, "", "VJQV"},
		{2, "", "dv8A"},
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
	sta := dataset.Structure{Format: "json", Schema: dataset.BaseSchemaObject}
	g, _ := NewGenerator(&sta, AssignSeed, AssignMaxLen)
	cases := []struct {
		index int
		key   string
		value string
	}{
		{0, "VJQV", "gltBH"},
		{0, "0", "dv8A"},
		{0, "", ""},
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
