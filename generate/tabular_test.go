package generate

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

// Compile time check that Generator satisfies the EntryReader interace.
var _ dsio.EntryReader = (*TabularGenerator)(nil)

func TestGeneratorForBaseSchemaArray(t *testing.T) {
	cases := []struct {
		index int
		key   string
		value interface{}
	}{
		{0, "", []interface{}{"gltBH"}},
		{1, "", []interface{}{"VJQV"}},
		{2, "", []interface{}{"dv8A"}},
	}

	st := &dataset.Structure{Format: "json", Schema: map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type": "array",
			"items": []interface{}{
				map[string]interface{}{"type": "string", "title": "col_one,"},
			},
		},
	}}

	g, err := NewTabularGenerator(st, AssignSeed, AssignMaxLen)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if diff := cmp.Diff(st, g.Structure()); diff != "" {
		t.Errorf("expected returned structure to match input. (-want +got)P:\n%s", diff)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d", c.index), func(t *testing.T) {
			e, _ := g.ReadEntry()
			if e.Index != c.index {
				t.Errorf("case %d index mismatch. expected: %d. got: %d", i, c.index, e.Index)
			}
			if e.Key != c.key {
				t.Errorf("case %d key mismatch. expected: %s. got: %s", i, c.key, e.Key)
			}
			if diff := cmp.Diff(c.value, e.Value); diff != "" {
				t.Errorf("case result mismatch. (-want +got):\n%s", diff)
			}
		})
	}

}
