package dataset

import "github.com/qri-io/datatype"

// Field represents a field descriptor
type Field struct {
	Name         Name              `json:"name"`
	Title        string            `json:"title,omitempty"`
	Type         datatype.Type     `json:"type,omitempty"`
	MissingValue interface{}       `json:"missingValue,omitempty"`
	Format       string            `json:"format,omitempty"`
	Description  string            `json:"description,omitempty"`
	Constraints  *FieldConstraints `json:"constraints,omitempty"`
}

func NewShortField(name, dataType string) *Field {
	return &Field{
		Name: Name(name),
		Type: datatype.TypeFromString(dataType),
	}
}

// FieldKey allows a field key to be either a string or object
type FieldKey []string

type ForeignKey struct {
	Fields FieldKey `json:"fields"`
	// Reference
}

type FieldConstraints struct {
	Required  *bool         `json:"required,omitempty"`
	MinLength *int64        `json:"minLength,omitempty"`
	MaxLength *int64        `json:"maxLength,omitempty"`
	Unique    *bool         `json:"unique,omitempty"`
	Pattern   string        `json:"pattern,omitempty"`
	Minimum   interface{}   `json:"minimum,omitempty"`
	Maximum   interface{}   `json:"maximum,omitempty"`
	Enum      []interface{} `json:"enum,omitempty"`
}
