package dataset

import (
	"encoding/json"
	"fmt"
)

// Current version of the specification
const version = "0.0.1"

// VersionNumber is a semantic major.minor.patch
// TODO - make Version enforce this format
type VersionNumber string

// User is a placholder for talking about people, groups, organizations
type User struct {
	Fullname string `fn,omitempty`
	Email    string `email,omitempty`
}

// License represents a legal licensing agreement
type License struct {
	Type string `json:"type"`
	Url  string `json:"url"`
}

// private struct for marshaling
type _license License

// MarshalJSON satisfies the json.Marshaller interface
func (l License) MarshalJSON() ([]byte, error) {
	if l.Type != "" && l.Url == "" {
		return []byte(fmt.Sprintf(`"%s"`, l.Type)), nil
	}

	return json.Marshal(_license(l))
}

// UnmarshalJSON satisfies the json.Unmarshaller interface
func (l *License) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*l = License{Type: s}
		return nil
	}

	_l := &_license{}
	if err := json.Unmarshal(data, _l); err != nil {
		return fmt.Errorf("error parsing license from json: %s", err.Error())
	}
	*l = License(*_l)

	return nil
}

// VariableName is a string that conforms to standard variable naming conventions
// must start with a letter, no spaces
// TODO - we're not really using this much, consider depricating, or using properly
type VariableName string

// MarshalJSON satisfies the json.Marshaller interface
func (name VariableName) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, name)), nil
}

// UnmarshalJSON satisfies the json.Unmarshaller interface
func (name *VariableName) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("type should be a string, got %s", data)
	}

	if alphaNumericRegex.MatchString(s) {
		return fmt.Errorf("variable name must contain only letters, numbers, '_' or '-', and start with a letter")
	}

	*name = VariableName(s)
	return nil
}

// Citation is a place that this dataset drew it's information from
type Citation struct {
	Name  string `json:"name,omitempty"`
	Url   string `json:"url,omitempty"`
	Email string `json:"email,omitempty"`
}

// Theme is pulled from the Project Open Data Schema version 1.1
type Theme struct {
	Description     string `json:"description,omitempty"`
	DisplayName     string `json:"display_name,omitempty"`
	ImageDisplayUrl string `json:"image_display_url,omitempty"`
	Id              string `json:"id,omitempty"`
	Name            string `json:"name,omitempty"`
	Title           string `json:"title,omitempty"`
}
