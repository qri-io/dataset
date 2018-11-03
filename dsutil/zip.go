package dsutil

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/qri-io/dataset"
)

// UnzipDatasetBytes is a convenince wrapper for UnzipDataset
func UnzipDatasetBytes(zipData []byte, dsp *dataset.DatasetPod) error {
	return UnzipDataset(bytes.NewReader(zipData), int64(len(zipData)), dsp)
}

// UnzipDataset reads a zip file from a filename and returns a full dataset with components
func UnzipDataset(r io.ReaderAt, size int64, dsp *dataset.DatasetPod) error {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return err
	}

	contents, err := unzipGetContents(zr)
	if err != nil {
		return err
	}

	fileData, ok := contents["dataset.json"]
	if !ok {
		return fmt.Errorf("no dataset.json found in the provided zip")
	}
	if err = json.Unmarshal(fileData, dsp); err != nil {
		return err
	}

	// TODO - do a smarter iteration for body format
	if bodyData, ok := contents["body.json"]; ok {
		dsp.BodyBytes = bodyData
		dsp.BodyPath = ""
	}
	if bodyData, ok := contents["body.csv"]; ok {
		dsp.BodyBytes = bodyData
		dsp.BodyPath = ""
	}
	if bodyData, ok := contents["body.cbor"]; ok {
		dsp.BodyBytes = bodyData
		dsp.BodyPath = ""
	}

	if tfScriptData, ok := contents["transform.star"]; ok {
		if dsp.Transform == nil {
			dsp.Transform = &dataset.TransformPod{}
		}
		dsp.Transform.ScriptBytes = tfScriptData
		dsp.Transform.ScriptPath = ""
	}

	if vizScriptData, ok := contents["viz.html"]; ok {
		if dsp.Viz == nil {
			dsp.Viz = &dataset.Viz{}
		}
		dsp.Viz.ScriptBytes = vizScriptData
		dsp.Viz.ScriptPath = ""
	}

	// Get ref to existing dataset
	refText, ok := contents["ref.txt"]
	if !ok {
		return fmt.Errorf("no ref.txt found in the provided zip")
	}
	refStr := string(refText)
	atPos := strings.Index(refStr, "@")
	if atPos == -1 {
		return fmt.Errorf("invalid dataset ref: no '@' found")
	}
	// Get name and peername
	datasetName := refStr[:atPos]
	sepPos := strings.Index(datasetName, "/")
	if sepPos == -1 {
		return fmt.Errorf("invalid dataset name: no '/' found")
	}
	dsp.Peername = datasetName[:sepPos]
	dsp.Name = datasetName[sepPos+1:]
	return nil
}

// UnzipGetContents is a generic zip-unpack to a map of filename: contents
// with contents represented as strings
func UnzipGetContents(data []byte) (map[string]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	contents, err := unzipGetContents(zr)
	if err != nil {
		return nil, err
	}

	res := map[string]string{}
	for k, val := range contents {
		res[k] = string(val)
	}
	return res, nil
}

// unzipGetContents reads a zip file's contents and returns a map from filename to file data
func unzipGetContents(zr *zip.Reader) (map[string][]byte, error) {
	// Create a map from filenames in the zip to their json encoded contents.
	contents := make(map[string][]byte)
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		data, err := ioutil.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		contents[f.Name] = data
	}
	return contents, nil
}
