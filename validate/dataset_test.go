package validate

import (
	"strings"
	"testing"

	"github.com/qri-io/dataset"
)

func TestDataset(t *testing.T) {
	cm := &dataset.Commit{Title: "initial commit"}
	st := &dataset.Structure{Format: "json", Schema: map[string]interface{}{"type": "array"}}

	cases := []struct {
		ds  *dataset.Dataset
		err string
	}{
		{nil, ""},
		{&dataset.Dataset{}, "commit is required"},
		{&dataset.Dataset{Commit: cm, Structure: &dataset.Structure{}}, "structure: format is required"},
		{&dataset.Dataset{Commit: cm, Structure: st}, ""},
	}

	for i, c := range cases {
		err := Dataset(c.ds)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestCommit(t *testing.T) {
	cases := []struct {
		cm  *dataset.Commit
		err string
	}{
		{nil, ""},
		{&dataset.Commit{}, ""},
		{&dataset.Commit{Title: strings.Repeat("f", 150)}, "title is too long. 150 length exceeds 100 character limit"},
		{&dataset.Commit{Title: "message"}, ""},
	}

	for i, c := range cases {
		err := Commit(c.cm)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestStructure(t *testing.T) {
	cases := []struct {
		st  *dataset.Structure
		err string
	}{
		{nil, ""},
		{&dataset.Structure{}, "format is required"},
		{&dataset.Structure{Format: "csv"}, "csv data format requires a schema"},
		// {&dataset.Structure{Format: "csv"}, "schema: fields are required"},
		{&dataset.Structure{Format: "json", Schema: map[string]interface{}{"type": "array"}}, ""},
	}

	for i, c := range cases {
		err := Structure(c.st)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

// func TestSchema(t *testing.T) {
// 	cases := []struct {
// 		sh  *dataset.Schema
// 		err string
// 	}{
// 		{nil, ""},
// 		{&dataset.Schema{}, "fields are required"},
// 		{&dataset.Schema{Fields: []*dataset.Field{&dataset.Field{Name: "1"}}}, "fields: error: illegal name '1', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters"},
// 		{&dataset.Schema{Fields: []*dataset.Field{&dataset.Field{Name: "field", Type: vals.Float}}}, ""},
// 	}

// 	for i, c := range cases {
// 		err := Schema(c.sh)
// 		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
// 			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
// 			continue
// 		}
// 	}
// }

// func TestFields(t *testing.T) {
// 	if err := Fields(nil); err != nil {
// 		t.Errorf("expected nil response. got: %s", err.Error())
// 	}

// 	cases := []struct {
// 		input []string
// 		err   string
// 	}{
// 		{[]string{"abc", "12startsWithNumber"}, `error: illegal name '12startsWithNumber', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters`},
// 		{[]string{"abc", "$dollarsAtBeginning"}, `error: illegal name '$dollarsAtBeginning', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters`},
// 		{[]string{"abc", "Dollars$inTheMiddle"}, `error: illegal name 'Dollars$inTheMiddle', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters`},
// 		{[]string{"abc", ""}, `error: name cannot be empty`},
// 		{[]string{"abc", "No|pipes"}, `error: illegal name 'No|pipes', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters`},
// 		{[]string{"repeatedName", "repeatedName", "repeatedName"}, "error: cannot use the same name, 'repeatedName' more than once"},
// 	}
// 	for i, c := range cases {
// 		s := fieldsTestHelper(c.input)
// 		err := Fields(s)
// 		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
// 			t.Errorf("case [%d] error mismatch. expected: '%s', got: '%s'", i, c.err, err)
// 			continue
// 		}
// 	}
// }

// // takes a slice of strings and creates a pointer to a Structure
// // containing a schema containing those fields
// func fieldsTestHelper(s []string) []*dataset.Field {
// 	fields := []*dataset.Field{}
// 	for _, fieldName := range s {
// 		newField := dataset.Field{Name: fieldName}
// 		fields = append(fields, &newField)
// 	}
// 	return fields
// }
