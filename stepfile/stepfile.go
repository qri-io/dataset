// Package stepfile provides utilities for reading and writing an ordered set of
// transform steps to and from a flat file representation
//
// A stepfile file consists of one or more steps of input text separated by
// "---" lines.
//
// Example:
//
//      "step"
//      ---
//      "another step"
//      ---
//      "and another step"
package stepfile

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/qri-io/dataset"
)

// ReadFile opens a stepfile and returns steps
func ReadFile(filename string) (steps []*dataset.TransformStep, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Read(f)
}

// Read consumes a reader into steps
func Read(r io.Reader) (steps []*dataset.TransformStep, err error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	for _, chunk := range strings.Split(string(data), "\n---\n") {
		steps = append(steps, &dataset.TransformStep{
			Script: chunk,
		})
	}
	return steps, nil
}

// Write prints transform steps as a stepfile to a writer
func Write(steps []*dataset.TransformStep, w io.Writer) error {
	for i, step := range steps {
		if err := writeStepScript(step, w); err != nil {
			return err
		}
		if i != len(steps)-1 {
			w.Write([]byte("\n---\n"))
		}
	}
	return nil
}

func writeStepScript(s *dataset.TransformStep, w io.Writer) error {
	if r, ok := s.Script.(io.Reader); ok {
		if closer, ok := s.Script.(io.Closer); ok {
			defer closer.Close()
		}
		_, err := io.Copy(w, r)
		return err
	}

	switch v := s.Script.(type) {
	case string:
		_, err := w.Write([]byte(v))
		return err
	case []byte:
		_, err := w.Write(v)
		return err
	}
	return fmt.Errorf("unrecognized script type: %T", s.Script)
}
