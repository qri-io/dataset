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
  secrets:
    a: b
    c: d
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

const yamlBadData0 = `---
meta:
  title: Bad data that has incorrect case-sensitivity
  description: Yaml uses case-sensitive fields
transForm:
  config:
    foo: bar
structure:
  format: json
`

const yamlBadData1 = `---
meta:
  title: Bad data that has an unknown field
  description: Yaml strict parsing will reject unknown fields
  tags:
  - cat
  - dog
transForm:
  config:
    foo: bar
structure:
  format: json
`

func TestUnmarshalYAMLFailCaseSensitive(t *testing.T) {
	dsp := &dataset.DatasetPod{}
	err := UnmarshalYAMLDatasetPod([]byte(yamlBadData0), dsp)
	if err == nil {
		t.Error("Expected an error parsing bad yaml that relies on case-sensitivity")
		return
	}
}

func TestUnmarshalYAMLFailUnknownField(t *testing.T) {
	dsp := &dataset.DatasetPod{}
	err := UnmarshalYAMLDatasetPod([]byte(yamlBadData1), dsp)
	if err == nil {
		t.Error("Expected an error parsing bad yaml that has an unknown field")
		return
	}
}
