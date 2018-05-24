package dsutil

import (
	"testing"

	"github.com/qri-io/dataset"
)

const yamlData = `---
meta:
  title: EPA TRI Basic Summary
  description: A few key fields pulled from EPA TRI Basic data for 2016
transform:
  config:
    foo: bar
structure:
  format: json
  schema:
    type: array
    items:
      type: array
      items:
      - title: Year
        maxLength: 4
        type: string
        description: "The Reporting Year - Year the chemical was released or managed as waste"
      - title: "TRI Facility ID"
        maxLength: 15
        type: string
        description: "The TRI Facility Identification Number assigned by EPA/TRI"
      - title: Facility Name
        maxLength: 62
        type: string
        description: "Facility Name"
`

func TestUnmarshalYAMLDatasetPod(t *testing.T) {
	dsp := &dataset.DatasetPod{}
	if err := UnmarshalYAMLDatasetPod([]byte(yamlData), dsp); err != nil {
		t.Error(err.Error())
		return
	}

	ds := &dataset.Dataset{}
	if err := ds.Decode(dsp); err != nil {
		t.Error(err.Error())
		return
	}
}
