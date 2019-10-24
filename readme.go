package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/qri-io/qfs"
)

// Readme stores configuration data related to representing a dataset as a
// visualization
type Readme struct {
	// Format designates the visualization configuration syntax. Only supported
	// formats are "html" and "md"
	Format string `json:"format,omitempty"`
	// Path is the location of a readme, transient
	// derived
	Path string `json:"path,omitempty"`
	// Qri should always be "rm:0"
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
	// RenderedPath is the path to the file rendered using the readme script and the body
	RenderedPath string `json:"renderedPath,omitempty"`
}

// NewReadmeRef creates an empty struct with it's internal path set
func NewReadmeRef(path string) *Readme {
	return &Readme{Path: path}
}

// DropTransientValues removes values that cannot be recorded when the
// dataset is rendered immutable, usually by storing it in a cafs
func (r *Readme) DropTransientValues() {
	r.Path = ""
	r.ScriptBytes = nil
}

// DropDerivedValues resets all set-on-save fields to their default values
func (r *Readme) DropDerivedValues() {
	r.Qri = ""
	r.Path = ""
}

// InlineScriptFile opens the script file, reads its contents, and assigns it to scriptBytes.
func (r* Readme) InlineScriptFile(ctx context.Context, resolver qfs.PathResolver) error {
	if resolver == nil {
		return nil
	}
	err := r.OpenScriptFile(ctx, resolver)
	if err != nil {
		return err
	}
	file := r.ScriptFile()
	if file == nil {
		return nil
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	r.ScriptBytes = data
	r.ScriptPath = ""
	return nil
}

// OpenScriptFile generates a byte stream of script data prioritizing creating an
// in-place file from ScriptBytes when defined, fetching from the
// passed-in resolver otherwise
func (r *Readme) OpenScriptFile(ctx context.Context, resolver qfs.PathResolver) (err error) {
	if r.ScriptBytes != nil {
		r.scriptFile = qfs.NewMemfileBytes("readme.md", r.ScriptBytes)
		return nil
	}

	if r.ScriptPath == "" {
		// nothing to resolve
		return nil
	}

	if resolver == nil {
		return ErrNoResolver
	}
	r.scriptFile, err = resolver.Get(ctx, r.ScriptPath)
	return err
}

// SetScriptFile assigns the unexported scriptFile
func (r *Readme) SetScriptFile(file qfs.File) {
	r.scriptFile = file
}

// OpenRenderedFile generates a byte stream of the rendered data
func (r *Readme) OpenRenderedFile(ctx context.Context, resolver qfs.PathResolver) (err error) {
	if r.RenderedPath == "" {
		// nothing to resolve
		return nil
	}

	if resolver == nil {
		return ErrNoResolver
	}
	r.renderedFile, err = resolver.Get(ctx, r.RenderedPath)
	return err
}

// SetRenderedFile assigns the unexported renderedFile
func (r *Readme) SetRenderedFile(file qfs.File) {
	r.renderedFile = file
}

// ScriptFile exposes scriptFile if one is set. Callers that use the file in any
// way (eg. by calling Read) should consume the entire file and call Close
func (r *Readme) ScriptFile() qfs.File {
	return r.scriptFile
}

// RenderedFile exposes renderedFile if one is set. Callers that use the file in any
// way (eg. by calling Read) should consume the entire file and call Close
func (r *Readme) RenderedFile() qfs.File {
	return r.renderedFile
}

// IsEmpty checks to see if Readme has any fields other than the internal path
func (r *Readme) IsEmpty() bool {
	return r.Format == "" &&
		r.ScriptBytes == nil &&
		r.ScriptPath == "" &&
		r.RenderedPath == ""
}

// Assign collapses all properties of a group of structures on to one this is
// directly inspired by Javascript's Object.assign
func (r *Readme) Assign(readmeConfigs ...*Readme) {
	for _, rs := range readmeConfigs {
		if rs == nil {
			continue
		}

		if rs.Format != "" {
			r.Format = rs.Format
		}
		if rs.Path != "" {
			r.Path = rs.Path
		}
		if rs.Qri != "" {
			r.Qri = rs.Qri
		}
		if rs.ScriptBytes != nil {
			r.ScriptBytes = rs.ScriptBytes
		}
		if rs.scriptFile != nil {
			r.scriptFile = rs.scriptFile
		}
		if rs.ScriptPath != "" {
			r.ScriptPath = rs.ScriptPath
		}
		if rs.RenderedPath != "" {
			r.RenderedPath = rs.RenderedPath
		}
	}
}

// MarshalJSON satisfies the json.Marshaler interface
func (r *Readme) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal
	// to a string instead
	if r.Path != "" && r.IsEmpty() {
		return json.Marshal(r.Path)
	}
	if r.Qri == "" {
		r.Qri = KindReadme.String()
	}

	return r.MarshalJSONObject()
}

type _readme Readme

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (r *Readme) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*r = Readme{Path: s}
		return nil
	}

	_r := _readme{}
	if err := json.Unmarshal(data, &_r); err != nil {
		return err
	}
	if _r.Qri == "" {
		_r.Qri = KindReadme.String()
	}
	*r = Readme(_r)
	return nil
}

// UnmarshalReadme tries to extract a resource type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalReadme(v interface{}) (*Readme, error) {
	switch q := v.(type) {
	case *Readme:
		return q, nil
	case Readme:
		return &q, nil
	case []byte:
		r := Readme{}
		err := json.Unmarshal(q, &r)
		return &r, err
	default:
		err := fmt.Errorf("couldn't parse Readme, value is invalid type")
		return nil, err
	}
}

// MarshalJSONObject always marshals to a json Object, even if Readme is empty or
// a reference
func (r *Readme) MarshalJSONObject() ([]byte, error) {
	data := map[string]interface{}{
		"qri": r.Qri,
	}

	if r.Format != "" {
		data["format"] = r.Format
	}
	if r.ScriptBytes != nil {
		data["scriptBytes"] = r.ScriptBytes
	}
	if r.ScriptPath != "" {
		data["scriptPath"] = r.ScriptPath
	}
	if r.RenderedPath != "" {
		data["renderedPath"] = r.RenderedPath
	}

	return json.Marshal(data)
}
