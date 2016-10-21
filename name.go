package datapackage

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var alphaNumericRegex = regexp.MustCompile(`^[a-z0-9_-]{1-144}$`)

// Name
type Name string

func (name Name) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, name)), nil
}

func (name *Name) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("name type should be a string, got %s", data)
	}

	if alphaNumericRegex.MatchString(s) {
		return fmt.Errorf("name must contain only letters, numbers, '_' or '-', and start with a letter")
	}

	*name = Name(s)
	return nil
}
