// Package dsviz performs actions on viz components of a dataset
package dsviz

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/alecthomas/template"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/qfs"
)

const htmlTmplName = "index.html"

// Render executes the viz component of a dataset, returning a "rendered" output that is the result
// of running the viz script template, providing the dataset as input
// the provided dataset must be loaded, with script files ready for consumption
// Render replaces any file readers it consumes, making the dataset safe for reuse after calling
func Render(ds *dataset.Dataset) (qfs.File, error) {
	if ds.Viz == nil {
		return nil, fmt.Errorf("no viz component")
	}
	if ds.Viz.Format != "html" {
		return nil, fmt.Errorf("render format must be 'html'")
	}
	return renderHTML(ds)
}

func renderHTML(ds *dataset.Dataset) (qfs.File, error) {
	script := ds.Viz.ScriptFile()
	// tee the viz file to avoid losing script data
	vizScriptBuf := &bytes.Buffer{}
	tr := io.TeeReader(script, vizScriptBuf)
	teedVizScriptFile := qfs.NewMemfileReader(script.FileName(), tr)

	tmplBytes, err := ioutil.ReadAll(teedVizScriptFile)
	if err != nil {
		return nil, fmt.Errorf("reading template data: %s", err.Error())
	}

	// restore consumed script file
	ds.Viz.SetScriptFile(qfs.NewMemfileReader(script.FileName(), vizScriptBuf))

	// parse template
	tmpl, err := template.New(htmlTmplName).Parse(string(tmplBytes))
	if err != nil {
		return nil, fmt.Errorf("parsing template: %s", err.Error())
	}

	// load all body data
	bodyFile := ds.BodyFile()
	bodyBytesBuf := &bytes.Buffer{}
	tr = io.TeeReader(bodyFile, bodyBytesBuf)
	rr, err := dsio.NewEntryReader(ds.Structure, tr)
	if err != nil {
		return nil, fmt.Errorf("error allocating data reader: %s", err)
	}

	bodyEntries, err := readEntries(rr)
	if err != nil {
		return nil, err
	}

	// make sure there's a meta component, lots of templates reference meta
	if ds.Meta == nil {
		ds.Meta = &dataset.Meta{}
	}

	ds.Body = bodyEntries

	// do the render
	tmplBuf := &bytes.Buffer{}
	if err := tmpl.Execute(tmplBuf, ds); err != nil {
		return nil, err
	}

	ds.Body = nil
	// restore body file
	ds.SetBodyFile(qfs.NewMemfileReader(bodyFile.FileName(), bodyBytesBuf))
	return qfs.NewMemfileReader(htmlTmplName, tmplBuf), nil
}

// readEntries reads entries and returns them as a native go array or map
func readEntries(reader dsio.EntryReader) (interface{}, error) {
	obj := make(map[string]interface{})
	array := make([]interface{}, 0)

	tlt, err := dsio.GetTopLevelType(reader.Structure())
	if err != nil {
		return nil, err
	}

	for i := 0; ; i++ {
		val, err := reader.ReadEntry()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		if tlt == "object" {
			obj[val.Key] = val.Value
		} else {
			array = append(array, val.Value)
		}
	}

	if tlt == "object" {
		return obj, nil
	}
	return array, nil
}
