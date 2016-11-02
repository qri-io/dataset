package dataset

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// this regex makes sure we have snake_case.addresses.with.dot_separators.1_and_only_alphanumeric_characters
var pathRegex = regexp.MustCompile(`^[a-z0-9-_/]+(\.[a-z0-9-_/]+)?$`)

// check for a valid namespce address
func ValidPathString(s string) bool {
	return pathRegex.MatchString(s)
}

// a address is a string slice that divides the global namspace
type Path []string

// Create a new address from one or more strings. all strings are divided by any dot separators.
// So the internal array would map as:
// 	NewPath("user.dataset","table") => ["user","dataset","table"]
// Which is the eqivelent to:
// 	NewPath("user", "dataset", "table") => ["user", "dataset", "table"]
func NewPath(strs ...string) (p Path) {
	for _, str := range strs {
		for _, s := range strings.Split(str, ".") {
			p = append(p, s)
		}
	}

	return
}

// Conform to stringer interface
func (p Path) String() string {
	return strings.Join(p, ".")
}

func (a Path) Endpoint() string {
	return "/" + strings.Join(a, "/")
}

func (ns *Path) UnmarshallJSON(data []byte) error {
	// Extract the string from data.
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("User type should be a string, got %s", data)
	}

	s = strings.TrimSpace(s)
	if !ValidPathString(s) {
		return fmt.Errorf("Invalid address: %s", s)
	}

	*ns = NewPath(s)
	return nil
}

func (p Path) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, p.String())), nil
}

func (ad *Path) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Data Type should be a string, got %s", data)
	}

	*ad = NewPath(s)
	return nil
}
