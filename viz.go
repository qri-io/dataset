package dataset

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/ipfs/go-datastore"
)

// Viz stores configuration data related to representing a dataset as a
// visualization
type Viz struct {
	// private storage for reference to this object
	path datastore.Key
	// Qri should always be "vc:0"
	Qri Kind
	// Format designates the visualization configuration syntax. currently the
	// only supported syntax is "html"
	Format string
	// Script is a reader of raw script data
	Script io.Reader `json:"_"`
	// ScriptPath is the path to the script that created this
	ScriptPath string `json:"script,omitempty"`
}

// Path gives the internal path reference for this structure
func (v *Viz) Path() datastore.Key {
	return v.path
}

// NewVizRef creates an empty struct with it's internal path set
func NewVizRef(path datastore.Key) *Viz {
	return &Viz{path: path}
}

// IsEmpty checks to see if Viz has any fields other than the internal path
func (v *Viz) IsEmpty() bool {
	return v.Format == "" && v.ScriptPath == ""
}

// SetPath sets the internal path property of a Viz
// Use with caution. most callers should never need to call SetPath
func (v *Viz) SetPath(path string) {
	if path == "" {
		v.path = datastore.Key{}
	} else {
		v.path = datastore.NewKey(path)
	}
}

// Assign collapses all properties of a group of structures on to one this is
// directly inspired by Javascript's Object.assign
func (v *Viz) Assign(visConfigs ...*Viz) {
	for _, vs := range visConfigs {
		if vs == nil {
			continue
		}

		if vs.path.String() != "" {
			v.path = vs.path
		}
		if vs.Qri != "" {
			v.Qri = vs.Qri
		}
		if vs.Format != "" {
			v.Format = vs.Format
		}
		if vs.ScriptPath != "" {
			v.ScriptPath = vs.ScriptPath
		}
	}
}

// vizPod is a private struct for marshaling into & out of.
// fields must remain sorted in lexographical order
type vizPod struct {
	Format     string `json:"format,omitempty"`
	Qri        Kind   `json:"qri,omitempty"`
	ScriptPath string `json:"scriptPath,omitempty"`
}

// MarshalJSON satisfies the json.Marshaler interface
func (v *Viz) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal
	// to a string instead
	if v.path.String() != "" && v.IsEmpty() {
		return v.path.MarshalJSON()
	}
	return v.MarshalJSONObject()
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (v *Viz) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*v = Viz{path: datastore.NewKey(s)}
		return nil
	}

	vp := &vizPod{}
	if err := json.Unmarshal(data, vp); err != nil {
		return err
	}

	*v = Viz{
		Format:     vp.Format,
		Qri:        vp.Qri,
		ScriptPath: vp.ScriptPath,
	}
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
		log.Debug(err.Error())
		return nil, err
	}
}

// MarshalJSONObject always marshals to a json Object, even if Viz is empty or
// a reference
func (v *Viz) MarshalJSONObject() ([]byte, error) {
	data := map[string]interface{}{}
	data["qri"] = KindViz

	if v.Format != "" {
		data["format"] = v.Format
	}
	if v.ScriptPath != "" {
		data["scriptPath"] = v.ScriptPath
	}

	return json.Marshal(data)
}
