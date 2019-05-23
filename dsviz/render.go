package dsviz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/qfs"
)

const htmlTmplName = "index.html"

// Render executes the viz component of a dataset, returning a resulting file of
// running the viz script template file, with the host dataset as input. The
// provided dataset must be fully deserialized, with all files Opened
// Render replaces any file readers it consumes, making the dataset safe for
// reuse after calling render
func Render(ds *dataset.Dataset) (qfs.File, error) {
	if ds.Viz == nil {
		return nil, fmt.Errorf("no viz component")
	}
	if ds.Viz.Format != "html" {
		return nil, fmt.Errorf("render format must be 'html'")
	}
	return renderHTML(ds)
}

// PredefinedHTMLTemplates is a key-value set of templates to be add to HTML
// renders. {{ block }} elements defined in any templates here will be available
// to passed-in dataset template files used during Render
var PredefinedHTMLTemplates map[string]string

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

	vizDs, err := vizDataset(ds)
	if err != nil {
		return nil, err
	}

	tmpl := template.New(htmlTmplName)

	tmpl.Funcs(template.FuncMap{
		"ds": func() map[string]interface{} {
			return vizDs
		},
		"bodyEntries":    bodyEntriesFunc(ds),
		"allBodyEntries": allBodyEntriesFunc(ds),
		"filesize": func(n float64) string {
			return printByteInfo(int(n))
		},
		"title": func() string {
			if ds.Meta != nil && ds.Meta.Title != "" {
				return ds.Meta.Title
			}
			return fmt.Sprintf("%s/%s", ds.Peername, ds.Name)
		},
		// TODO (b5):
		// {{ timeParse }}
		// 	parse a timestamp string, returning a golang *time.Time struct
		// {{ timeFormat }}
		// 	convert the textual representation of the datetime into the specified
		// 	format using a template date
	})

	for name, tmplText := range PredefinedHTMLTemplates {
		tmpl.New(name).Parse(tmplText)
	}

	if tmpl, err = tmpl.Parse(string(tmplBytes)); err != nil {
		return nil, fmt.Errorf("parsing template: %s", err.Error())
	}

	// do the render
	tmplBuf := &bytes.Buffer{}
	if err := tmpl.Execute(tmplBuf, ds); err != nil {
		return nil, err
	}

	return qfs.NewMemfileReader(htmlTmplName, tmplBuf), nil
}

func vizDataset(ds *dataset.Dataset) (vizDs map[string]interface{}, err error) {
	data, err := json.Marshal(ds)
	if err != nil {
		return nil, err
	}
	vizDs = map[string]interface{}{}
	err = json.Unmarshal(data, &vizDs)
	return
}

func allBodyEntriesFunc(ds *dataset.Dataset) func() (interface{}, error) {
	return func() (interface{}, error) {
		return bodyEntries(ds, 0, -1)
	}
}

func bodyEntriesFunc(ds *dataset.Dataset) func(offset int, limit int) (interface{}, error) {
	return func(offset, limit int) (interface{}, error) {
		return bodyEntries(ds, offset, limit)
	}
}

func bodyEntries(ds *dataset.Dataset, offset, limit int) (interface{}, error) {
	if ds.Structure == nil {
		return nil, fmt.Errorf("can't get_body. dataset has no structure component")
	}

	// load all body data
	bodyFile := ds.BodyFile()
	bodyBytesBuf := &bytes.Buffer{}
	tr := io.TeeReader(bodyFile, bodyBytesBuf)
	rr, err := dsio.NewEntryReader(ds.Structure, tr)
	if err != nil {
		return nil, fmt.Errorf("error allocating data reader: %s", err)
	}

	if offset >= 0 && limit >= 0 {
		rr = &dsio.PagedReader{
			Reader: rr,
			Offset: offset,
			Limit:  limit,
		}
	}

	bodyEntries, err := readEntries(rr)
	if err != nil {
		return nil, err
	}

	defer func() {
		// restore body file
		ds.SetBodyFile(qfs.NewMemfileReader(bodyFile.FileName(), bodyBytesBuf))
	}()

	return bodyEntries, nil
}

// readEntries reads entries and returns them as a native go array or map
// COPIED from github.com/qri-io/qri/base/entries.go
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

const (
	bite = 1 << (10 * iota)
	kilobyte
	megabyte
	gigabyte
	terabyte
	petabyte
	exabyte
	zettabyte
	yottabyte
)

// COPIED (b5): github.com/qri-io/qri/cmd/print.go
func printByteInfo(n int) string {
	// Use 64-bit ints to support platforms on which int is not large enough to
	// represent the constants below (exabyte, petabyte, etc).
	// For example: Raspberry Pi running arm6.
	l := int64(n)
	length := struct {
		name  string
		value int64
	}{"", 0}

	switch {
	case l >= petabyte:
		length.name = "PB"
		length.value = l / petabyte
	case l >= terabyte:
		length.name = "TB"
		length.value = l / terabyte
	case l >= gigabyte:
		length.name = "GB"
		length.value = l / gigabyte
	case l >= megabyte:
		length.name = "MB"
		length.value = l / megabyte
	case l >= kilobyte:
		length.name = "KB"
		length.value = l / kilobyte
	default:
		length.name = "byte"
		length.value = l
	}
	if length.value != 1 {
		length.name += "s"
	}
	return fmt.Sprintf("%v %s", length.value, length.name)
}
