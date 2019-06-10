package dsviz

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/qfs"
)

func TestRenderHTML(t *testing.T) {
	if _, err := Render(&dataset.Dataset{}); err == nil {
		t.Error("expected ds with no viz to error")
	}
	if _, err := Render(&dataset.Dataset{Viz: &dataset.Viz{Format: "WebGL"}}); err == nil {
		t.Error("expected non-html viz format to error")
	}

	tcs, err := dstest.LoadTestCases("testdata")
	if err != nil {
		t.Fatal(err)
	}

	tc := tcs["custom"]
	rendered, err := Render(tc.Input)
	if err != nil {
		t.Fatal(err)
	}
	checkResult(t, tc, rendered)

	tc = tcs["default"]
	if rendered, err = Render(tc.Input); err != nil {
		t.Fatal(err)
	}
	checkResult(t, tc, rendered)
}

func checkResult(t *testing.T, tc dstest.TestCase, rendered qfs.File) {
	got, err := ioutil.ReadAll(rendered)
	if err != nil {
		t.Error(err)
	}

	rf, err := tc.RenderedFile()
	if err != nil {
		t.Error(err)
	}

	expect, err := ioutil.ReadAll(rf)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(expect, got) {
		t.Errorf("result mismatch. expected:\n%s\ngot:\n%s", string(expect), string(got))
	}
}

func TestPredefinedHTML(t *testing.T) {
	PredefinedHTMLTemplates = map[string]string{
		"hi friend":  `{{ block "special_sauce" . }}<h1>special sauce</h1>{{ end }}`,
		"bye friend": `{{ block "groovy_gravy" . }}<p>alright, what?</p>{{ end }}`,
	}

	ds := &dataset.Dataset{
		Viz: &dataset.Viz{Format: "html"},
	}
	ds.Viz.SetScriptFile(qfs.NewMemfileBytes("template.html", []byte(`{{ block "special_sauce" .}}{{end}}{{ block "groovy_gravy" .}}{{end}}`)))

	rendered, err := Render(ds)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ioutil.ReadAll(rendered)
	if err != nil {
		t.Error(err)
	}

	expect := []byte(`<h1>special sauce</h1><p>alright, what?</p>`)
	if !bytes.Equal(expect, got) {
		t.Errorf("result mismatch. expected:\n%s\ngot:\n%s", string(expect), string(got))
	}
}

func TestHTMLFuncs(t *testing.T) {
	tmpl := `
{{ title }}
{{ filesize 0 }}
{{ filesize 1000 }}
{{ filesize 1000000 }}
{{ filesize 1000000000 }}
{{ filesize 1000000000000 }}
{{ filesize 1000000000000000 }}
{{ filesize 1000000000000000000 }}`

	ds := &dataset.Dataset{
		Name:     "a",
		Peername: "b",
		Viz:      &dataset.Viz{Format: "html"},
	}
	ds.Viz.SetScriptFile(qfs.NewMemfileBytes("template.html", []byte(tmpl)))

	if _, err := Render(ds); err != nil {
		t.Fatal(err)
	}

	// getBody when there's no body should fail
	ds = &dataset.Dataset{
		Name:     "a",
		Peername: "b",
		Viz:      &dataset.Viz{Format: "html"},
	}
	ds.Viz.SetScriptFile(qfs.NewMemfileBytes("template.html", []byte(`{{ getBody }}`)))
	if _, err := Render(ds); err == nil {
		t.Errorf("expected render to error")
	}
}

func TestIsType(t *testing.T) {
	tmpl := `{{- $data := allBodyEntries -}}
{{- if isType $data.obj "object" }}object{{ end }}
{{- if isType $data.obj "array" }}NO!{{ end }}
{{ if isType $data.arr "array" }}array{{ end }}
{{- if isType $data.arra "object" }}NO!{{ end }}
{{ if isType $data.str "string" }}string{{ end }}
{{- if isType $data.str "boolean" }}NO!{{ end }}
{{ if isType $data.bool "boolean" }}boolean{{ end }}
{{ if isType $data.num "number" }}number{{ end }}
`

	body := `{
		"obj" : {},
		"arr": [],
		"str": "",
		"bool": false,
		"null": null,
		"num": 4
	}`

	ds := &dataset.Dataset{
		Name:     "a",
		Peername: "b",
		Viz:      &dataset.Viz{Format: "html"},
		Structure: &dataset.Structure{
			Format: "json",
			Schema: dataset.BaseSchemaObject,
		},
	}
	ds.SetBodyFile(qfs.NewMemfileBytes("body.json", []byte(body)))
	ds.Viz.SetScriptFile(qfs.NewMemfileBytes("template.html", []byte(tmpl)))

	file, err := Render(ds)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	exp := `object
array
string
boolean
number
`

	if string(got) != exp {
		t.Errorf("response mismatch.\nexpected:\n'%s'\ngot:\n'%s'", exp, string(got))
	}
}
