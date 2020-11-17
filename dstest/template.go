package dstest

import (
	"bytes"
	"testing"
	"text/template"
)

// Template executes & returns a template string, failing the test if the
// template fails to compile
func Template(t *testing.T, tmplStr string, data interface{}) string {
	t.Helper()
	tmpl, err := template.New("tmpl").Parse(tmplStr)
	if err != nil {
		t.Fatalf("error parsing dstest template: %s", err)
	}

	w := &bytes.Buffer{}
	if err := tmpl.Execute(w, data); err != nil {
		t.Fatalf("error executing dstest template: %s", err)
	}

	return w.String()
}
