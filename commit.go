package dataset

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/qri/repo/profile"
)

// CommitMsg encapsulates information about changes to a dataset in
// relation to other entries in a given history. CommitMsg is intended
// to be directly analagous to the concept of a Commit Message in the
// git version control system
type CommitMsg struct {
	path    datastore.Key
	Author  *profile.Profile `json:"author,omitempty"`
	Message string           `json:"message"`
}

// NewCommitMsgRef creates an empty struct with it's
// internal path set
func NewCommitMsgRef(path datastore.Key) *CommitMsg {
	return &CommitMsg{path: path}
}

// IsEmpty checks to see if any fields are filled out
func (cm *CommitMsg) IsEmpty() bool {
	return cm.Message == "" && cm.Author == nil
}

// Path returns the internal path of this commitMsg
func (cm *CommitMsg) Path() datastore.Key {
	return cm.path
}

// MarshalJSON implements the json.Marshaler interface for CommitMsg
// Empty CommitMsg instances with a non-empty path marshal to their path value
// otherwise, CommitMsg marshals to an object
func (cm *CommitMsg) MarshalJSON() ([]byte, error) {
	if cm.path.String() != "" && cm.IsEmpty() {
		return cm.path.MarshalJSON()
	}
	m := &_commitMsg{
		Author:  cm.Author,
		Message: cm.Message,
	}
	return json.Marshal(m)
}

// internal struct for json unmarshaling
type _commitMsg CommitMsg

// UnmarshalJSON implements json.Unmarshaller for CommitMsg
func (cm *CommitMsg) UnmarshalJSON(data []byte) error {
	// first check to see if this is a valid path ref
	var path string
	if err := json.Unmarshal(data, &path); err == nil {
		*cm = CommitMsg{path: datastore.NewKey(path)}
		return nil
	}

	m := _commitMsg{}
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("error unmarshling dataset: %s", err.Error())
	}

	*cm = CommitMsg(m)
	return nil
}

// UnmarshalCommitMsg tries to extract a dataset type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalCommitMsg(v interface{}) (*CommitMsg, error) {
	switch r := v.(type) {
	case *CommitMsg:
		return r, nil
	case CommitMsg:
		return &r, nil
	case []byte:
		cm := &CommitMsg{}
		err := json.Unmarshal(r, cm)
		return cm, err
	default:
		return nil, fmt.Errorf("couldn't parse commitMsg, value is invalid type")
	}
}
