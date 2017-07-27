package dataset

import (
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-datastore"
	"regexp"
)

var alphaNumericRegex = regexp.MustCompile(`^[a-z0-9_-]{1-144}$`)

// Metadata is stored separately from prescriptive metadata stored in Resource structs
// to maximize overlap of the formal query & resource definitions.
// This also creates space for subjective claims about datasets, and allows metadata
// to take on a higher frequency of change in contrast to the underlying definition.
// In addition, descriptive metadata can and should be author attributed
// associating descriptive claims about a resource with a cyptographic keypair which
// may represent a person, group of people, or software.
// This metadata format is also subject to massive amounts of change.
// Design goals should include making this compatible with the DCAT spec,
// with the one major exception that hashes are acceptable in place of urls.
type Metadata struct {
	Title        string        `json:"title,omitempty"`
	Url          string        `json:"url,omitempty"`
	Readme       string        `json:"readme,omitempty"`
	Author       *User         `json:"author,omitempty"`
	Image        string        `json:"image,omitempty"`
	Description  string        `json:"description,omitempty"`
	Homepage     string        `json:"homepage,omitempty"`
	IconImage    string        `json:"icon_image,omitempty"`
	PosterImage  string        `json:"poster_image,omitempty"`
	License      *License      `json:"license,omitempty"`
	Version      Version       `json:"version,omitempty"`
	Keywords     []string      `json:"keywords,omitempty"`
	Contributors []*User       `json:"contributors,omitempty"`
	Subject      datastore.Key `json:"subject,omitempty"`
}

// User is a placholder for talking about people, groups, organizations
type User string

// License represents a legal licensing agreement
type License struct {
	Type string `json:"type"`
	Url  string `json:"url,omitempty"`
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
		return err
	}
	*l = License(*_l)

	return nil
}

// VariableName is a string that conforms to standard variable naming conventions
// must start with a letter, no spaces
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

// Version is a semantic major.minor.patch
// TODO - make Version enforce this format
type Version string
