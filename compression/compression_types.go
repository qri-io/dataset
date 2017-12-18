// Package compression is a horrible hack & should be replaced
// as soon as humanly possible
package compression

import (
	"encoding/json"
	"fmt"
)

// Type represents a type of byte compression
type Type int

const (
	// None speficies no compression
	None Type = iota
	Gzip
	Tar
)

// Names maps the name of a hash to codes
var Names = map[Type]string{
	None: "",
	Gzip: "gzip",
	Tar:  "tar",
}

// Codes maps a hash code to it's name
var Codes = map[string]Type{
	"": None,
}

// ParseTypeString returns a compression type for a given string
func ParseTypeString(s string) (t Type, err error) {
	t, ok := Codes[s]
	if !ok {
		err = fmt.Errorf("invalid compression type %q", s)
		t = None
	}

	return
}

// String satisfies the stringer interface
func (t Type) String() string {
	return Names[t]
}

// MarshalJSON satisfies the json.Marshaler interface
func (t Type) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, t.String())), nil
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (t *Type) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Compression type value should be a string, got %s", data)
	}

	_t, err := ParseTypeString(s)
	if err != nil {
		return err
	}

	*t = _t
	return nil
}
