package dataset

import "encoding/json"

// Stats is a component that contains statistical metadata about the body of a
// dataset
type Stats struct {
	Path  string      `json:"path,omitempty"`
	Qri   string      `json:"qri,omitempty"`
	Stats interface{} `json:"stats,omitempty"`
}

// NewStatsRef creates an empty struct with it's path set
func NewStatsRef(path string) *Stats {
	return &Stats{Path: path}
}

// DropDerivedValues resets all set-on-save fields to their default values
func (sa *Stats) DropDerivedValues() {
	sa.Qri = ""
	sa.Path = ""
}

// IsEmpty checks to see if stats has any fields other than Path set
func (sa *Stats) IsEmpty() bool {
	return sa.Stats == nil
}

// Assign collapses all properties of a group of Stats components onto one
func (sa *Stats) Assign(sas ...*Stats) {
	for _, s := range sas {
		if s == nil {
			continue
		}

		if s.Stats != nil {
			sa.Stats = s.Stats
		}
		if s.Path != "" {
			sa.Path = s.Path
		}
		if s.Qri != "" {
			sa.Qri = s.Qri
		}
	}
}

// _stats is a private struct for marshaling into & out of.
// fields must remain sorted in lexographical order
type _stats Stats

// MarshalJSON satisfies the json.Marshaler interface
func (sa Stats) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal to
	// a string instead
	if sa.Path != "" && sa.IsEmpty() {
		return json.Marshal(sa.Path)
	}
	return sa.MarshalJSONObject()
}

// MarshalJSONObject always marshals to a json Object, even if Stats is empty or
// a reference
func (sa Stats) MarshalJSONObject() ([]byte, error) {
	kind := sa.Qri
	if kind == "" {
		kind = KindStats.String()
	}

	return json.Marshal(&_stats{
		Stats: sa.Stats,
		Path:  sa.Path,
		Qri:   kind,
	})
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (sa *Stats) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*sa = Stats{Path: s}
		return nil
	}

	_sa := _stats{}
	if err := json.Unmarshal(data, &_sa); err != nil {
		return err
	}

	*sa = Stats(_sa)
	return nil
}
