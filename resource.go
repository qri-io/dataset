package datapackage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/qri-io/jsontable"
)

type Resource struct {
	// one of these is required
	Url  string `json:"url,omitempty"`
	Path string `json:"path,omitempty"`
	Data []byte `json:"data,omitempty"`

	// should exist
	Name Name `json:"name,omitempty"`

	// optional
	Title       string           `json:"title,omitempty"`
	Description string           `json:"description,omitempty"`
	Image       string           `json:"image,omitempty"`
	Format      DataFormat       `json:"format,omitempty"`
	Mediatype   string           `json:"mediatype,omitempty"`
	Encoding    string           `json:"encoding,omitempty"`
	Bytes       int64            `json:"bytes,omitempty"`
	Hash        string           `json:"hash,omitempty"`
	Schema      *jsontable.Table `json:"schema,omitempty"`
	Sources     []*Source        `json:"sources,omitempty"`
	License     *License         `json:"license,omitempty"`
}

// FetchBytes grabs the actual byte data that this resource represents
// path is the path to the datapackage, and only needed if using the "path"
// resource param
func (r *Resource) FetchBytes(path string) ([]byte, error) {
	if len(r.Data) > 0 {
		return r.Data, nil
	} else if r.Path != "" {
		return ioutil.ReadFile(filepath.Join(path, r.Path))
	} else if r.Url != "" {
		res, err := http.Get(r.Url)
		if err != nil {
			return nil, err
		}

		defer res.Body.Close()
		return ioutil.ReadAll(res.Body)
	}

	return nil, fmt.Errorf("resource %s doesn't contain a url, path, or data field to read from", r.Name)
}

func (r *Resource) Reader() (io.Reader, error) {
	if len(r.Data) > 0 {
		return ioutil.NopCloser(bytes.NewBuffer(r.Data)), nil
	} else if r.Path != "" {
		return os.Open(r.Path)
	} else if r.Url != "" {
		res, err := http.Get(r.Url)
		if err != nil {
			return nil, err
		}
		return res.Body, nil
	}
	return nil, fmt.Errorf("resource %s doesn't contain a url, path, or data field to read from", r.Name)
}

type dataWriter struct {
	buffer  *bytes.Buffer
	onClose func([]byte)
}

func (w dataWriter) Write(p []byte) (n int, err error) {
	return w.Write(p)
}

func (w dataWriter) Close() error {
	data, err := json.Marshal(w.buffer.Bytes())
	if err != nil {
		w.onClose(data)
	}
	return err
}

func (r *Resource) Writer() (io.WriteCloser, error) {
	if len(r.Data) > 0 {
		return dataWriter{buffer: bytes.NewBuffer(r.Data), onClose: func(data []byte) { r.Data = data }}, nil
	} else if r.Path != "" {
		return os.Open(r.Path)
	} else if r.Url != "" {
		return nil, fmt.Errorf("can't write to url-based resource: %s", r.Url)
	}

	return nil, fmt.Errorf("resource %s doesn't contain a path or data field to write to", r.Name)
}

// separate type for marshalling into
type _resource Resource

// UnmarhalJSON can marshal in two forms: just an id string, or an object containing a full data model
func (r *Resource) UnmarshalJSON(data []byte) error {
	_r := _resource{}
	if err := json.Unmarshal(data, &_r); err != nil {
		return err
	}

	*r = Resource(_r)

	if r.Url == "" && r.Path == "" && len(r.Data) == 0 {
		if r.Name != "" {
			return fmt.Errorf("resource %s must specify one of url, path, or data fields", r.Name)
		}
	}
	// TODO - more validation:
	// 	* make sure only one field is specified
	// 	* check for valid urls

	return nil
}
