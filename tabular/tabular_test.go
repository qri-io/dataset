package tabular

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestColumnsFromJSONSchema(t *testing.T) {
	good := []struct {
		description string
		input       string
		expect      Columns
	}{
		{"one column array wrapper", `{
			"type" : "array",
			"items": {
				"type": "array",
				"items": [
					{ "title" : "column_1", "type": "string", "description":"the first column" }
				]
			}
		}`, Columns{
			{Title: "column_1", Description: "the first column", Type: &ColType{"string"}},
		}},
		{"one column array wrapper with validation", `{
			"type" : "array",
			"items": {
				"type": "array",
				"items": [
					{ "title" : "rating", "type": ["number", "null"], "description":"0-5 rating", "max": 5, "min": 0 }
				]
			}
		}`, Columns{
			{Title: "rating", Description: "0-5 rating", Type: &ColType{"number", "null"}, Validation: map[string]interface{}{"max": float64(5), "min": float64(0)}},
		}},
		// {"one column object wrapper", `{
		// 	"type": "object",
		// 	"properties": {
		// 		"column_1": {
		// 			"type": "array",
		// 			"title": "column_1",
		// 			"description": "the first column",
		// 			"items": { "type": "string" }
		// 		}
		// 	}
		// }`, []Column{
		// 	{Title: "column_1", Description: "the first column", Type: "string"},
		// }},
	}

	for _, c := range good {
		t.Run(c.description, func(t *testing.T) {
			input := map[string]interface{}{}
			if err := json.Unmarshal([]byte(c.input), &input); err != nil {
				t.Fatal(err)
			}

			got, problems, err := ColumnsFromJSONSchema(input)
			if err != nil {
				t.Fatal(err)
			}

			if len(problems) != 0 {
				t.Errorf("unexpected problems: %s", problems)
			}

			if diff := cmp.Diff(c.expect, got); diff != "" {
				t.Errorf("result mismatch. (-want +got):\n%s", diff)
			}
		})
	}

	bad := []struct {
		input string
		err   string
	}{
		{`{}`, "invalid tabular schema: top-level 'type' field is required"},
		{`{ "type": "string" }`, "invalid tabular schema: 'string' is not a valid type to describe the top level of a tablular schema"},
		{`{ "type": "array" }`, "invalid tabular schema: top level 'items' property must be an object"},
		{`{ "type": "array", "items": { "type" : "string" }}`, "invalid tabular schema: items.items must be an array"},
		{`{ "type": "array", "items": { "type" : "array", "items": { "type": "array"}}}`, "invalid tabular schema: items.items must be an array"},
	}
	for _, c := range bad {
		t.Run(fmt.Sprintf("bad_case_%s", c.err), func(t *testing.T) {
			input := map[string]interface{}{}
			if err := json.Unmarshal([]byte(c.input), &input); err != nil {
				t.Fatal(err)
			}

			_, _, err := ColumnsFromJSONSchema(input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if diff := cmp.Diff(c.err, err.Error()); diff != "" {
				t.Errorf("result mismatch. (-want +got):\n%s", diff)
			}
		})
	}

	problems := []struct {
		description string
		input       string
		problems    []string
	}{
		{"false column",
			`{ "type": "array", "items": { "type" : "string", "items": [false] }}`,
			[]string{"col. 0 schema should be an object"},
		},
		{"missing title",
			`{ "type": "array", "items": { "type" : "string", "items": [{"type": "string"}] }}`,
			[]string{"col. 0 title is not set"},
		},
		{"missing type",
			`{ "type": "array", "items": { "type" : "string", "items": [{"title": "a_column"}] }}`,
			[]string{"col, 0 type is not set, defaulting to string"},
		},
	}
	for _, c := range problems {
		t.Run(fmt.Sprintf("problem_%s", c.description), func(t *testing.T) {
			input := map[string]interface{}{}
			if err := json.Unmarshal([]byte(c.input), &input); err != nil {
				t.Fatal(err)
			}

			_, problems, err := ColumnsFromJSONSchema(input)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if diff := cmp.Diff(c.problems, problems); diff != "" {
				t.Errorf("result mismatch. (-want +got):\n%s", diff)
			}
		})
	}
}

func TestColumnsJSON(t *testing.T) {
	val := `[{"title":"foo","type":["string","number"]},{"title":"bar","type":"string"}]`
	cols := &Columns{}
	if err := json.Unmarshal([]byte(val), cols); err != nil {
		t.Errorf("unmarshal error: %s", err)
	}

	data, err := json.Marshal(cols)
	if err != nil {
		t.Errorf("marshal error: %s", err)
	}

	if diff := cmp.Diff(val, string(data)); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestColumnsTitles(t *testing.T) {
	cols := Columns{
		Column{Title: "foo"},
		Column{Title: ""},
		Column{Title: "🔥"},
	}

	got := cols.Titles()
	expect := []string{"foo", "", "🔥"}
	if diff := cmp.Diff(expect, got); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestValidMachineTitles(t *testing.T) {
	good := []struct {
		description string
		cols        Columns
	}{
		{"single title",
			Columns{
				Column{Title: "foo"},
			},
		},
	}
	for _, c := range good {
		t.Run(c.description, func(t *testing.T) {
			if err := c.cols.ValidMachineTitles(); err != nil {
				t.Error(err)
			}
		})
	}

	bad := []struct {
		err  string
		cols Columns
	}{
		{"invalid tabular schema: column names have problems:\ncol. 0 name '???' is not a valid column name",
			Columns{
				Column{Title: "???"},
			},
		},
		{"invalid tabular schema: column names have problems:\ncol. 1 name 'a' is not unique",
			Columns{
				Column{Title: "a"},
				Column{Title: "a"},
			},
		},
	}

	for _, c := range bad {
		t.Run(c.err, func(t *testing.T) {
			err := c.cols.ValidMachineTitles()
			if err == nil {
				t.Error("expected error, got nil")
			}
			if diff := cmp.Diff(c.err, err.Error()); diff != "" {
				t.Errorf("error mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
