package dataset

import (
  "encoding/json"
  "fmt"
  "github.com/ipfs/go-datastore"
)

// VisConfig stores configuration data related to representing a dataset as a visualization
type VisConfig struct {
  // private storage for reference to this object
  path datastore.Key
  // Kind should always be "qri:vc:0"
  Kind Kind
  // Format designates the visualization configuration syntax
  Format string
  // Visualizations lists concrete configuration details. Top level must always
  // be a slice, even when only one visualization is present. The top level is left as an empty
  // interface to allow custom go structures later on, but we should initially require this to be
  // a []map[string]interface{}, with a type assertion that fails if it's anything else. That check
  // should happen in dsfs.CreateDataset
  Visualizations interface{}
  // DataPath is the least worked out part, but I'm imagining we should
  // borrow some of our thinking from dataset.Transformation to place here, with the
  // goal of being able to specify all details necessary to generate visualizations
  // from some sort of executable code designated by this hash?
  // DataPath string
}

// Path gives the internal path reference for this structure
func (v *VisConfig) Path() datastore.Key {
  return v.path
}

// NewVisConfigRef creates an empty struct with it's
// internal path set
func NewVisConfigRef(path datastore.Key) *VisConfig {
  return &VisConfig{path: path}
}

// IsEmpty checks to see if VisConfig has any fields other than the internal path
func (v *VisConfig) IsEmpty() bool {
  return v.Format == "" && v.Visualizations == nil
  // v.Format == "" && v.DataPath == "" && v.Visualizations == nil
}

// Assign collapses all properties of a group of structures on to one
// this is directly inspired by Javascript's Object.assign
func (v *VisConfig) Assign(visConfigs ...*VisConfig) {
  for _, vs := range visConfigs {
    if vs == nil {
      continue
    }

    if vs.path.String() != "" {
      v.path = vs.path
    }
    if vs.Kind != "" {
      v.Kind = vs.Kind
    }
    if vs.Format != "" {
      v.Format = vs.Format
    }
    // if vs.DataPath != "" {
    //   v.DataPath = vs.DataPath
    // }
    if vs.Visualizations != nil {
      v.Visualizations = vs.Visualizations
    }
  }
}

// _visconfig is a private struct for marshaling into & out of.
// fields must remain sorted in lexographical order
type _visconfig struct {
  // DataPath       string      `json:"datapath,omitempty"`
  Format         string      `json:"format,omitempty"`
  Kind           Kind        `json:"kind,omitempty"`
  Visualizations interface{} `json:"visualizations,omitempty"`
}

// MarshalJSON satisfies the json.Marshaler interface
func (v *VisConfig) MarshalJSON() ([]byte, error) {
  // if we're dealing with an empty object that has a path specified, marshal to a string instead
  if v.path.String() != "" && v.IsEmpty() {
    return v.path.MarshalJSON()
  }

  kind := v.Kind
  if kind == "" {
    kind = KindVisConfig
  }

  return json.Marshal(&_visconfig{
    // DataPath:       v.DataPath,
    Format:         v.Format,
    Kind:           kind,
    Visualizations: v.Visualizations,
  })
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (v *VisConfig) UnmarshalJSON(data []byte) error {
  var s string
  if err := json.Unmarshal(data, &s); err == nil {
    *v = VisConfig{path: datastore.NewKey(s)}
    return nil
  }

  _v := &_visconfig{}
  if err := json.Unmarshal(data, _v); err != nil {
    return err
  }

  *v = VisConfig{
    // DataPath:       _v.DataPath,
    Format:         _v.Format,
    Kind:           _v.Kind,
    Visualizations: _v.Visualizations,
  }
  return nil
}

// UnmarshalVisConfig tries to extract a resource type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalVisConfig(v interface{}) (*VisConfig, error) {
  switch q := v.(type) {
  case *VisConfig:
    return q, nil
  case VisConfig:
    return &q, nil
  case []byte:
    visConfig := &VisConfig{}
    err := json.Unmarshal(q, visConfig)
    return visConfig, err
  default:
    return nil, fmt.Errorf("couldn't parse VisConfig, value is invalid type")
  }
}

// MarshalJSONObject always marshals to a json Object, even if VisConfig is empty or a reference
func (v *VisConfig) MarshalJSONObject() ([]byte, error) {
  data := map[string]interface{}{}
  data["kind"] = KindVisConfig

  if v.Format != "" {
    data["format"] = v.Format
  }
  if v.Visualizations != nil {
    data["visualizations"] = v.Visualizations
  }

  return json.Marshal(data)
}
