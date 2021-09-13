package stepfile

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qri-io/dataset"
)

func TestRead(t *testing.T) {
	cases := []struct {
		inputFilename  string
		expectFilename string
	}{
		{"steps.txt", "steps.json"},
	}

	for _, c := range cases {
		t.Run(c.inputFilename, func(t *testing.T) {
			in := filepath.Join("./testdata", c.inputFilename)
			expect := []*dataset.TransformStep{}
			f, err := os.Open(filepath.Join("./testdata", c.expectFilename))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.NewDecoder(f).Decode(&expect); err != nil {
				t.Fatal(err)
			}
			f.Close()

			got, err := ReadFile(in)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(expect, got); diff != "" {
				t.Errorf("result mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("errors", func(t *testing.T) {
		if _, err := ReadFile("unknown"); err == nil {
			t.Error("expected error reading unknown file")
		}
	})
}

func TestWrite(t *testing.T) {
	cases := []struct {
		inputFilename  string
		expectFilename string
	}{
		{"steps.json", "steps.txt"},
	}

	for _, c := range cases {
		t.Run(c.inputFilename, func(t *testing.T) {
			data, err := ioutil.ReadFile(filepath.Join("./testdata", c.expectFilename))
			if err != nil {
				t.Fatal(err)
			}
			expect := string(data)

			input := []*dataset.TransformStep{}
			f, err := os.Open(filepath.Join("./testdata", c.inputFilename))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.NewDecoder(f).Decode(&input); err != nil {
				t.Fatal(err)
			}
			f.Close()

			buf := &bytes.Buffer{}
			if err := Write(input, buf); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(expect, buf.String()); diff != "" {
				t.Errorf("result mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("write from a reader", func(t *testing.T) {
		steps := []*dataset.TransformStep{
			{Script: bytes.NewBuffer([]byte("oh hai"))},
			{Script: []byte("my friend")},
		}
		buf := &bytes.Buffer{}
		if err := Write(steps, buf); err != nil {
			t.Error(err)
		}
		expect := "oh hai\n---\nmy friend"
		if diff := cmp.Diff(expect, buf.String()); diff != "" {
			t.Errorf("result mismatch. (-want +got):\n %s", diff)
		}
	})

	t.Run("bad scripts", func(t *testing.T) {
		steps := []*dataset.TransformStep{
			{Script: 2},
		}
		buf := &bytes.Buffer{}
		if err := Write(steps, buf); err == nil {
			t.Error("expected error, got none")
		}
	})
}
