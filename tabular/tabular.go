// Package tabular defines functions for working with rectangular datasets.
// qri positions tabular data as a special shape that comes with additional
// constraints. This package defines the methods necessary to enforce and
// interpret those constraints
package tabular

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrInvalidTabularSchema is a base type for schemas that don't work as tables
// all parsing errors in this package can be errors.Is() to this one
var ErrInvalidTabularSchema = errors.New("invalid tabular schema")

// Columns is an ordered list of column information
type Columns []Column

// Titles gives just column titles as a slice of strings
func (cols Columns) Titles() []string {
	titles := make([]string, len(cols))
	for i, col := range cols {
		titles[i] = col.Title
	}
	return titles
}

var validMachineTitle = regexp.MustCompile(`^[a-zA-Z_$][a-zA-Z_$0-9]*$`)

// ValidMachineTitles confirms column titles are valid for machine-readability
// using column titles that parse as proper variable names, and unique titles
// across the column set
func (cols Columns) ValidMachineTitles() error {

	var problems []string
	set := map[string]struct{}{}

	for i, col := range cols {
		t := col.Title
		if !validMachineTitle.MatchString(t) {
			problems = append(problems, fmt.Sprintf("col. %d name '%s' is not a valid column name", i, t))
		}
		if _, present := set[t]; present {
			problems = append(problems, fmt.Sprintf("col. %d name '%s' is not unique", i, t))
		}
		set[t] = struct{}{}
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w: column names have problems:\n%s", ErrInvalidTabularSchema, strings.Join(problems, "\n"))
	}
	return nil
}

// Column defines values associated with an index of each row of data
type Column struct {
	Title       string                 `json:"title"`
	Type        *ColType               `json:"type"`
	Description string                 `json:"description,omitempty"`
	Validation  map[string]interface{} `json:"validation,omitempty"`
}

// ColType implements type information for a tabular column. Column Types can
// be one or more strings enumerating accepted types
type ColType []string

// HasType ranges over the column types and returns true if the type is present
func (ct ColType) HasType(t string) bool {
	for _, x := range ct {
		if x == t {
			return true
		}
	}
	return false
}

// MarshalJSON encodes to string in the common case of a single type, an array
// of strings for a type enumeration
func (ct ColType) MarshalJSON() ([]byte, error) {
	switch len(ct) {
	case 0:
		return nil, nil
	case 1:
		return json.Marshal(ct[0])
	default:
		return json.Marshal([]string(ct))
	}
}

// UnmarshalJSON decodes string and string array data types
func (ct *ColType) UnmarshalJSON(p []byte) error {
	var str string
	if err := json.Unmarshal(p, &str); err == nil {
		*ct = ColType{str}
		return nil
	}

	var strs []string
	if err := json.Unmarshal(p, &strs); err == nil {
		*ct = ColType(strs)
		return nil
	}

	return fmt.Errorf("invalid data for ColType")
}

// ColumnsFromJSONSchema extracts column data from a jsonSchema object, erroring
// if the provided schema cannot be used to describe a table. a slice of problem
// strings describes non-breaking issues with the schema that should be
// addressed like missing column titles or column types
// the passed in schema must be a decoding of a json schema into default type
// mappings from the encoding/json package
func ColumnsFromJSONSchema(sch map[string]interface{}) (Columns, []string, error) {
	topLevelType, ok := sch["type"].(string)
	if !ok {
		msg := "top-level 'type' field is required"
		return nil, nil, fmt.Errorf("%w: %s", ErrInvalidTabularSchema, msg)
	}

	switch topLevelType {
	case "array":
		return arrayWrapperColumns(sch)
	case "object":
		return objectWrapperColumns(sch)
	default:
		msg := fmt.Sprintf("'%s' is not a valid type to describe the top level of a tablular schema", topLevelType)
		return nil, nil, fmt.Errorf("%w: %s", ErrInvalidTabularSchema, msg)
	}
}

func arrayWrapperColumns(sch map[string]interface{}) (Columns, []string, error) {
	var problems []string

	itemObj, ok := sch["items"].(map[string]interface{})
	if !ok {
		msg := "top level 'items' property must be an object"
		return nil, nil, fmt.Errorf("%w: %s", ErrInvalidTabularSchema, msg)
	}

	itemArr, ok := itemObj["items"].([]interface{})
	if !ok {
		msg := "items.items must be an array"
		return nil, nil, fmt.Errorf("%w: %s", ErrInvalidTabularSchema, msg)
	}

	cols := make([]Column, len(itemArr))
	for i, f := range itemArr {
		cols[i].Title = fmt.Sprintf("col_%d", i)
		cols[i].Type = &ColType{"string"}

		colSchema, ok := f.(map[string]interface{})
		if !ok {
			problems = append(problems, fmt.Sprintf("col. %d schema should be an object", i))
			continue
		}

		setTitle, setType := false, false
		for key, val := range colSchema {
			switch key {
			case "title":
				if title, ok := val.(string); ok {
					setTitle = true
					cols[i].Title = title
				}
			case "type":
				setType = true
				switch x := val.(type) {
				case string:
					cols[i].Type = &ColType{x}
				case []interface{}:
					types := ColType{}
					for _, v := range x {
						if t, ok := v.(string); ok {
							types = append(types, t)
						}
					}
					cols[i].Type = &types
				}
			case "description":
				if d, ok := val.(string); ok {
					cols[i].Description = d
				}
			default:
				if cols[i].Validation == nil {
					cols[i].Validation = map[string]interface{}{}
				}
				cols[i].Validation[key] = val
			}
		}

		if !setTitle {
			problems = append(problems, fmt.Sprintf("col. %d title is not set", i))
		}
		if !setType {
			problems = append(problems, fmt.Sprintf("col, %d type is not set, defaulting to string", i))
		}
	}

	return cols, problems, nil
}

func objectWrapperColumns(sch map[string]interface{}) (Columns, []string, error) {
	return nil, nil, fmt.Errorf("unfinished")
}
