package dsutil

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/qri-io/dataset"
)

// FormFileDataset extracts a dataset document from a http Request
func FormFileDataset(r *http.Request, dsp *dataset.DatasetPod) (cleanup func(), err error) {
	var rmFiles []*os.File
	cleanup = func() {
		// TODO - this needs to be removed ASAP in favor of constructing cafs.Files from form-file readers
		// There's danger this code could delete stuff not in temp directory if we're bad at our jobs.
		for _, f := range rmFiles {
			// TODO - log error?
			os.Remove(f.Name())
		}
	}

	datafile, dataHeader, err := r.FormFile("file")
	if err == http.ErrMissingFile {
		err = nil
	}
	if err != nil {
		err = fmt.Errorf("error opening dataset file: %s", err)
		return
	}
	if datafile != nil {
		switch strings.ToLower(filepath.Ext(dataHeader.Filename)) {
		case ".yaml", ".yml":
			var data []byte
			data, err = ioutil.ReadAll(datafile)
			if err != nil {
				err = fmt.Errorf("error reading dataset file: %s", err)
				return
			}
			if err = UnmarshalYAMLDatasetPod(data, dsp); err != nil {
				err = fmt.Errorf("error unmarshaling yaml file: %s", err)
				return
			}
		case ".json":
			if err = json.NewDecoder(datafile).Decode(dsp); err != nil {
				err = fmt.Errorf("error decoding json file: %s", err)
				return
			}
		}
	}

	tfFile, _, err := r.FormFile("transform")
	if err == http.ErrMissingFile {
		err = nil
	}
	if err != nil {
		err = fmt.Errorf("error opening transform file: %s", err)
		return
	}
	if tfFile != nil {
		var f *os.File
		// TODO - this assumes a starlark transform file
		if f, err = ioutil.TempFile("", "transform"); err != nil {
			return
		}
		rmFiles = append(rmFiles, f)
		io.Copy(f, tfFile)
		if dsp.Transform == nil {
			dsp.Transform = &dataset.TransformPod{}
		}
		dsp.Transform.Syntax = "starlark"
		dsp.Transform.ScriptPath = f.Name()
	}

	vizFile, _, err := r.FormFile("viz")
	if err == http.ErrMissingFile {
		err = nil
	}
	if err != nil {
		err = fmt.Errorf("error opening viz file: %s", err)
		return
	}
	if vizFile != nil {
		var f *os.File
		// TODO - this assumes an html viz file
		if f, err = ioutil.TempFile("", "viz"); err != nil {
			return
		}
		rmFiles = append(rmFiles, f)
		io.Copy(f, vizFile)
		if dsp.Viz == nil {
			dsp.Viz = &dataset.Viz{}
		}
		dsp.Viz.Format = "html"
		dsp.Viz.ScriptPath = f.Name()
	}

	dsp.Peername = r.FormValue("peername")
	dsp.Name = r.FormValue("name")
	dsp.BodyPath = r.FormValue("body_path")

	bodyfile, bodyHeader, err := r.FormFile("body")
	if err == http.ErrMissingFile {
		err = nil
	}
	if err != nil {
		err = fmt.Errorf("error opening body file: %s", err)
		return
	}
	if bodyfile != nil {
		var f *os.File
		path := filepath.Join(os.TempDir(), bodyHeader.Filename)
		if f, err = os.Create(path); err != nil {
			err = fmt.Errorf("error writing body file: %s", err.Error())
			return
		}
		rmFiles = append(rmFiles, f)
		io.Copy(f, bodyfile)
		f.Close()
		dsp.BodyPath = path
	}

	return
}
