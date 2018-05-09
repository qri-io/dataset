package dataset

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
)

// Commit encapsulates information about changes to a dataset in
// relation to other entries in a given history. Commit is intended
// to be directly analogous to the concept of a Commit Message in the
// git version control system
type Commit struct {
	path   datastore.Key
	Author *User `json:"author,omitempty"`
	// Message is an optional
	Message string `json:"message,omitempty"`
	// Qri is this commit's qri kind
	Qri Kind `json:"qri,omitempty"`
	// Signature is a base58 encoded privateKey signing of Title
	Signature string `json:"signature,omitempty"`
	// Time this dataset was created. Required.
	Timestamp time.Time `json:"timestamp"`
	// Title of the commit. Required.
	Title string `json:"title"`
}

// NewCommitRef creates an empty struct with it's
// internal path set
func NewCommitRef(path datastore.Key) *Commit {
	return &Commit{path: path}
}

// IsEmpty checks to see if any fields are filled out
func (cm *Commit) IsEmpty() bool {
	return cm.Title == "" && cm.Signature == "" && cm.Message == "" && cm.Author == nil && cm.Timestamp.IsZero()
}

// Path returns the internal path of this commitMsg
func (cm *Commit) Path() datastore.Key {
	return cm.path
}

// SetPath sets the internal path property of a commit
// Use with caution. most callers should never need to call SetPath
func (cm *Commit) SetPath(path string) {
	if path == "" {
		cm.path = datastore.Key{}
	} else {
		cm.path = datastore.NewKey(path)
	}
}

// Assign collapses all properties of a set of Commit onto one.
// this is directly inspired by Javascript's Object.assign
func (cm *Commit) Assign(msgs ...*Commit) {
	for _, m := range msgs {
		if m == nil {
			continue
		}

		if m.path.String() != "" {
			cm.path = m.path
		}
		if m.Author != nil {
			cm.Author = m.Author
		}
		if m.Title != "" {
			cm.Title = m.Title
		}
		if !m.Timestamp.IsZero() {
			cm.Timestamp = m.Timestamp
		}
		if m.Message != "" {
			cm.Message = m.Message
		}
		if m.Signature != "" {
			cm.Signature = m.Signature
		}
		if m.Qri.String() != "" {
			cm.Qri = m.Qri
		}
	}
}

// MarshalJSON implements the json.Marshaler interface for Commit
// Empty Commit instances with a non-empty path marshal to their path value
// otherwise, Commit marshals to an object
func (cm *Commit) MarshalJSON() ([]byte, error) {
	if cm.path.String() != "" && cm.IsEmpty() {
		return cm.path.MarshalJSON()
	}
	return cm.MarshalJSONObject()
}

// MarshalJSONObject always marshals to a json Object, even if meta is empty or a reference
func (cm *Commit) MarshalJSONObject() ([]byte, error) {
	kind := cm.Qri
	if kind == "" {
		kind = KindCommit
	}

	m := &_commitMsg{
		Author:    cm.Author,
		Message:   cm.Message,
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
		*cm = Commit{path: datastore.NewKey(path)}
		return nil
	}

	m := _commitMsg{}
	if err := json.Unmarshal(data, &m); err != nil {
		log.Debug(err.Error())
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
		log.Debug(err.Error())
		return nil, err
	}
}

// Encode creates a CommitPod from a Commit instance
func (cm Commit) Encode() *CommitPod {
	return &CommitPod{
		Author:    cm.Author,
		Message:   cm.Message,
		Path:      cm.Path().String(),
		Qri:       cm.Qri.String(),
		Signature: cm.Signature,
		Timestamp: cm.Timestamp,
		Title:     cm.Title,
	}
}

// Decode creates a Commit from a CommitPod instance
func (cm *Commit) Decode(cc *CommitPod) error {
	c := Commit{
		path:      datastore.NewKey(cc.Path),
		Author:    cc.Author,
		Message:   cc.Message,
		Signature: cc.Signature,
		Timestamp: cc.Timestamp,
		Title:     cc.Title,
	}

	if cc.Qri == KindCommit.String() {
		// TODO - this should respond to changes in CommitPod
		c.Qri = KindCommit
	} else if cc.Qri != "" {
		return fmt.Errorf("invalid commit 'qri' value: %s", cc.Qri)
	}

	*cm = c
	return nil
}

// CommitPod is a variant of Commit safe for serialization (encoding & decoding)
// to static formats. It uses only simple go types
type CommitPod struct {
	Author    *User     `json:"author,omitempty"`
	Message   string    `json:"message,omitempty"`
	Path      string    `json:"path,omitempty"`
	Qri       string    `json:"qri,omitempty"`
	Signature string    `json:"signature,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Title     string    `json:"title"`
}
