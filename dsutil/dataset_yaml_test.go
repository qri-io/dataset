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
  title: Bad data that has incorrect case-sensitivity, 'transForm' instead of 'transform'
  description: Yaml uses case-sensitive fields
transForm:
  config:
    foo: bar
structure:
  format: json
`

const yamlBadData1 = `---
meta:
  title: Bad data that has an unknown field 'tags'
  description: Yaml strict parsing will reject unknown fields
  tags:
  - cat
  - dog
transform:
  config:
    foo: bar
structure:
  format: json
`

func TestUnmarshalYAMLDataset(t *testing.T) {
	ds := &dataset.Dataset{}
	if err := UnmarshalYAMLDataset([]byte(yamlData), ds); err != nil {
		t.Error(err.Error())
		return
	}

	if ds.Transform.Secrets["a"] != "b" {
		t.Error("expected transform.secrets.a to equal 'b'")
		return
	}
}

func TestUnmarshalYAMLFailCaseSensitive(t *testing.T) {
	ds := &dataset.Dataset{}
	err := UnmarshalYAMLDataset([]byte(yamlBadData0), ds)
	if err == nil {
		t.Error("Expected an error parsing bad yaml that relies on case-sensitivity")
		return
	}
}

func TestUnmarshalYAMLFailUnknownField(t *testing.T) {
	ds := &dataset.Dataset{}
	err := UnmarshalYAMLDataset([]byte(yamlBadData1), ds)
	if err == nil {
		t.Error("Expected an error parsing bad yaml that has an unknown field")
		return
	}
}
