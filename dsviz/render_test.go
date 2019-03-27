package dsviz

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/qfs"
)

func TestRender(t *testing.T) {
	tcs, err := dstest.LoadTestCases("testdata")
	if err != nil {
		t.Fatal(err)
	}

	tc := tcs["custom"]
	rendered, err := Render(tc.Input)
	if err != nil {
		t.Error(err)
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
