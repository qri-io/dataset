package dataset

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/qri-io/qfs"
)

// Viz stores configuration data related to representing a dataset as a
// visualization
type Viz struct {
	// Format designates the visualization configuration syntax. currently the
	// only supported syntax is "html"
	Format string `json:"format,omitempty"`
	// Path is the location of a viz, transient
	// derived
	Path string `json:"path,omitempty"`
	// Qri should always be "vc:0"
	// derived
	Qri string `json:"qri,omitempty"`

	// script file reader, doesn't serialize
	scriptFile qfs.File
	// rendered file reader, doesn't serialize
	renderedFile qfs.File
	// ScriptBytes is for representing a script as a slice of bytes, transient
	ScriptBytes []byte `json:"scriptBytes,omitempty"`
	// ScriptPath is the path to the script that created this
	ScriptPath string `json:"scriptPath,omitempty"`
	// RenderedPath is the path to the file rendered using the viz script and the body
	RenderedPath string `json:"renderedPath,omitempty"`
}

// NewVizRef creates an empty struct with it's internal path set
func NewVizRef(path string) *Viz {
	return &Viz{Path: path}
}

// DropTransientValues removes values that cannot be recorded when the
// dataset is rendered immutable, usually by storing it in a cafs
func (v *Viz) DropTransientValues() {
	v.Path = ""
	v.ScriptBytes = nil
}

// DropDerivedValues resets all set-on-save fields to their default values
func (v *Viz) DropDerivedValues() {
	v.Qri = ""
	v.Path = ""
}

// OpenScriptFile generates a byte stream of script data prioritizing creating an
// in-place file from ScriptBytes when defined, fetching from the
// passed-in resolver otherwise
func (v *Viz) OpenScriptFile(ctx context.Context, resolver qfs.PathResolver) (err error) {
	if v.ScriptBytes != nil {
		v.scriptFile = qfs.NewMemfileBytes("template.html", v.ScriptBytes)
		return nil
	}

	if v.ScriptPath == "" {
		// nothing to resolve
		return nil
	}

	if resolver == nil {
		return ErrNoResolver
	}
	v.scriptFile, err = resolver.Get(ctx, v.ScriptPath)
	return err
}

// SetScriptFile assigns the unexported scriptFile
func (v *Viz) SetScriptFile(file qfs.File) {
	v.scriptFile = file
}

// OpenRenderedFile generates a byte stream of the rendered data
func (v *Viz) OpenRenderedFile(ctx context.Context, resolver qfs.PathResolver) (err error) {
	if v.RenderedPath == "" {
		// nothing to resolve
		return nil
	}

	if resolver == nil {
		return ErrNoResolver
	}
	v.renderedFile, err = resolver.Get(ctx, v.RenderedPath)
	return err
}

// SetRenderedFile assigns the unexported renderedFile
func (v *Viz) SetRenderedFile(file qfs.File) {
	v.renderedFile = file
}

// ScriptFile exposes scriptFile if one is set. Callers that use the file in any
// way (eg. by calling Read) should consume the entire file and call Close
func (v *Viz) ScriptFile() qfs.File {
	return v.scriptFile
}

// RenderedFile exposes renderedFile if one is set. Callers that use the file in any
// way (eg. by calling Read) should consume the entire file and call Close
func (v *Viz) RenderedFile() qfs.File {
	return v.renderedFile
}

// IsEmpty checks to see if Viz has any fields other than the internal path
func (v *Viz) IsEmpty() bool {
	return v.Format == "" &&
		v.ScriptBytes == nil &&
		v.ScriptPath == "" &&
		v.RenderedPath == ""
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
		if vs.scriptFile != nil {
			v.scriptFile = vs.scriptFile
		}
		if vs.ScriptPath != "" {
			v.ScriptPath = vs.ScriptPath
		}
		if vs.RenderedPath != "" {
			v.RenderedPath = vs.RenderedPath
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
	if _v.Qri == "" {
		_v.Qri = KindViz.String()
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
	if v.RenderedPath != "" {
		data["renderedPath"] = v.RenderedPath
	}

	return json.Marshal(data)
}
