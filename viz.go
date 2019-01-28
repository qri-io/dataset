package dataset

import (
	"encoding/json"
	"fmt"
)

// Viz stores configuration data related to representing a dataset as a
// visualization
type Viz struct {
	// Format designates the visualization configuration syntax. currently the
	// only supported syntax is "html"
	Format string `json:"format,omitempty"`
	// path is the location of a viz, transient
	Path string `json:"path,omitempty"`
	// Qri should always be "vc:0"
	Qri string `json:"qri,omitempty"`

	// TODO (b5): turn this into a method
	// Script is a reader of raw script data
	// Script io.Reader `json:"_"`

	// ScriptBytes is for representing a script as a slice of bytes
	ScriptBytes []byte `json:"scriptBytes,omitempty"`
	// ScriptPath is the path to the script that created this
	ScriptPath string `json:"scriptPath,omitempty"`
}

// NewVizRef creates an empty struct with it's internal path set
func NewVizRef(path string) *Viz {
	return &Viz{Path: path}
}

// IsEmpty checks to see if Viz has any fields other than the internal path
func (v *Viz) IsEmpty() bool {
	return v.Format == "" &&
		v.ScriptBytes == nil &&
		v.ScriptPath == ""
}

// Assign collapses all properties of a group of structures on to one this is
// directly inspired by Javascript's Object.assign
func (v *Viz) Assign(visConfigs ...*Viz) {
	for _, vs := range visConfigs {
		if vs == nil {
			continue
		}

		if vs.Format != "" {
			v.Format = vs.Format
		}
		if vs.Path != "" {
			v.Path = vs.Path
		}
		if vs.Qri != "" {
			v.Qri = vs.Qri
		}
		if vs.ScriptBytes != nil {
			v.ScriptBytes = vs.ScriptBytes
		}
		if vs.ScriptPath != "" {
			v.ScriptPath = vs.ScriptPath
		}
	}
}

// _viz is a private struct for marshaling into & out of.
type _viz Viz

// MarshalJSON satisfies the json.Marshaler interface
func (v *Viz) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal
	// to a string instead
	if v.Path != "" && v.IsEmpty() {
		return json.Marshal(v.Path)
	}
	if v.Qri == "" {
		v.Qri = KindViz.String()
	}

	return v.MarshalJSONObject()
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (v *Viz) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*v = Viz{Path: s}
		return nil
	}

	_v := _viz{}
	if err := json.Unmarshal(data, &_v); err != nil {
		return err
	}

	*v = Viz(_v)
	return nil
}

// UnmarshalViz tries to extract a resource type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalViz(v interface{}) (*Viz, error) {
	switch q := v.(type) {
	case *Viz:
		return q, nil
	case Viz:
		return &q, nil
	case []byte:
		visConfig := &Viz{}
		err := json.Unmarshal(q, visConfig)
		return visConfig, err
	default:
		err := fmt.Errorf("couldn't parse Viz, value is invalid type")
		return nil, err
	}
}

// MarshalJSONObject always marshals to a json Object, even if Viz is empty or
// a reference
func (v *Viz) MarshalJSONObject() ([]byte, error) {
	data := map[string]interface{}{
		"qri": v.Qri,
	}

	if v.Format != "" {
		data["format"] = v.Format
	}
	if v.ScriptBytes != nil {
		data["scriptBytes"] = v.ScriptBytes
	}
	if v.ScriptPath != "" {
		data["scriptPath"] = v.ScriptPath
	}

	return json.Marshal(data)
}
