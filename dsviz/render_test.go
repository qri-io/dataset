package dsviz

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/qri-io/dataset/dstest"
)

func TestRender(t *testing.T) {
	tcs, err := dstest.LoadTestCases("testdata")
	if err != nil {
		t.Fatal(err)
	}
	tc := tcs["basic"]

	rendered, err := Render(tc.Input)
	if err != nil {
		t.Error(err)
	}

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

	bytes.Equal(expect, got)
}
