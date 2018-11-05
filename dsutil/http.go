package dsutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/detect"
)

// FormFileDataset extracts a dataset document from a http Request
func FormFileDataset(r *http.Request, dsp *dataset.DatasetPod) (err error) {
	dsp.Peername = r.FormValue("peername")
	dsp.Name = r.FormValue("name")
	dsp.BodyPath = r.FormValue("body_path")

	datafile, dataHeader, err := r.FormFile("file")
	if err == http.ErrMissingFile {
		err = nil
	} else if err != nil {
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

	tfFile, tfHeader, err := r.FormFile("transform")
	if err == http.ErrMissingFile {
		err = nil
	} else if err != nil {
		err = fmt.Errorf("error opening transform file: %s", err)
		return
	}
	if tfFile != nil {
		var tfData []byte
		if tfData, err = ioutil.ReadAll(tfFile); err != nil {
			return
		}
		if dsp.Transform == nil {
			dsp.Transform = &dataset.TransformPod{}
		}
		dsp.Transform.Syntax = "starlark"
		dsp.Transform.ScriptBytes = tfData
		dsp.Transform.ScriptPath = tfHeader.Filename
	}

	vizFile, vizHeader, err := r.FormFile("viz")
	if err == http.ErrMissingFile {
		err = nil
	} else if err != nil {
		err = fmt.Errorf("error opening viz file: %s", err)
		return
	}
	if vizFile != nil {
		var vizData []byte
		if vizData, err = ioutil.ReadAll(vizFile); err != nil {
			return
		}
		if dsp.Viz == nil {
			dsp.Viz = &dataset.Viz{}
		}
		dsp.Viz.Format = "html"
		dsp.Viz.ScriptBytes = vizData
		dsp.Viz.ScriptPath = vizHeader.Filename
	}

	bodyfile, bodyHeader, err := r.FormFile("body")
	if err == http.ErrMissingFile {
		err = nil
	} else if err != nil {
		err = fmt.Errorf("error opening body file: %s", err)
		return
	}
	if bodyfile != nil {
		var bodyData []byte
		if bodyData, err = ioutil.ReadAll(bodyfile); err != nil {
			return
		}
		dsp.BodyPath = bodyHeader.Filename
		dsp.BodyBytes = bodyData

		if dsp.Structure == nil {
			// TODO - this is silly and should move into base.PrepareDataset funcs
			dsp.Structure = &dataset.StructurePod{}
			format, err := detect.ExtensionDataFormat(bodyHeader.Filename)
			if err != nil {
				return err
			}
			st, _, err := detect.FromReader(format, bytes.NewReader(dsp.BodyBytes))
			if err != nil {
				return err
			}
			dsp.Structure = st.Encode()
		}
	}

	return
}
