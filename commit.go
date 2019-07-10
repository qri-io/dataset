package dataset

import (
	"encoding/json"
	"fmt"
	"time"
)

// Commit encapsulates information about changes to a dataset in relation to
// other entries in a given history. Commit is directly analogous to the concept
// of a Commit Message in the git version control system. A full commit defines
// the administrative metadata of a dataset, answering "who made this
// dataset, when, and why"
type Commit struct {
	// Author of this commit
	Author *User `json:"author,omitempty"`
	// Message is an optional
	Message string `json:"message,omitempty"`
	// Path is the location of this commit, transient
	// derived
	Path string `json:"path,omitempty"`
	// Qri is this commit's qri kind
	// derived
	Qri string `json:"qri,omitempty"`
	// Signature is a base58 encoded privateKey signing of Title
	Signature string `json:"signature,omitempty"`
	// Time this dataset was created. Required.
	Timestamp time.Time `json:"timestamp"`
	// Title of the commit. Required.
	Title string `json:"title"`
}

// NewCommitRef creates an empty struct with it's
// internal path set
func NewCommitRef(path string) *Commit {
	return &Commit{Path: path}
}

// DropTransientValues removes values that cannot be recorded when the
// dataset is rendered immutable, usually by storing it in a cafs
func (cm *Commit) DropTransientValues() {
	cm.Path = ""
}

// DropDerivedValues removes values that cannot be recorded when the
// dataset is rendered immutable, usually by storing it in a cafs
func (cm *Commit) DropDerivedValues() {
	cm.Path = ""
	cm.Qri = ""
}

// IsEmpty checks to see if any fields are filled out other than Path and Qri
func (cm *Commit) IsEmpty() bool {
	return cm.Author == nil &&
		cm.Message == "" &&
		cm.Signature == "" &&
		cm.Timestamp.IsZero() &&
		cm.Title == ""
}

// Assign collapses all properties of a set of Commit onto one.
// this is directly inspired by Javascript's Object.assign
func (cm *Commit) Assign(msgs ...*Commit) {
	for _, m := range msgs {
		if m == nil {
			continue
		}

		if m.Author != nil {
			cm.Author = m.Author
		}
		if m.Message != "" {
			cm.Message = m.Message
		}
		if m.Path != "" {
			cm.Path = m.Path
		}
		if m.Qri != "" {
			cm.Qri = m.Qri
		}
		if m.Signature != "" {
			cm.Signature = m.Signature
		}
		if m.Title != "" {
			cm.Title = m.Title
		}
		if !m.Timestamp.IsZero() {
			cm.Timestamp = m.Timestamp
		}
	}
}

// MarshalJSON implements the json.Marshaler interface for Commit
// Empty Commit instances with a non-empty path marshal to their path value
// otherwise, Commit marshals to an object
func (cm *Commit) MarshalJSON() ([]byte, error) {
	if cm.Path != "" && cm.IsEmpty() {
		return json.Marshal(cm.Path)
	}
	return cm.MarshalJSONObject()
}

// MarshalJSONObject always marshals to a json Object, even if meta is empty or
// a reference
func (cm *Commit) MarshalJSONObject() ([]byte, error) {
	kind := cm.Qri
	if kind == "" {
		kind = KindCommit.String()
	}

	m := &_commitMsg{
		Author:    cm.Author,
		Message:   cm.Message,
		Path:      cm.Path,
		Qri:       kind,
		Signature: cm.Signature,
		Timestamp: cm.Timestamp,
		Title:     cm.Title,
	}
	return json.Marshal(m)
}

// internal struct for json unmarshaling
type _commitMsg Commit

// UnmarshalJSON implements json.Unmarshaller for Commit
func (cm *Commit) UnmarshalJSON(data []byte) error {
	// first check to see if this is a valid path ref
	var path string
	if err := json.Unmarshal(data, &path); err == nil {
		*cm = Commit{Path: path}
		return nil
	}

	m := _commitMsg{}
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("error unmarshling commit: %s", err.Error())
	}

	*cm = Commit(m)
	return nil
}

// UnmarshalCommit tries to extract a dataset type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalCommit(v interface{}) (*Commit, error) {
	switch r := v.(type) {
	case *Commit:
		return r, nil
	case Commit:
		return &r, nil
	case []byte:
		cm := &Commit{}
		err := json.Unmarshal(r, cm)
		return cm, err
	default:
		err := fmt.Errorf("couldn't parse commitMsg, value is invalid type")
		return nil, err
	}
}
