package dataset

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/ipfs/go-datastore"
)

func TestDatasetAssign(t *testing.T) {
	// TODO - expand test to check all fields
	cases := []struct {
		in *Dataset
	}{
		{&Dataset{path: datastore.NewKey("/a")}},
		{&Dataset{Structure: &Structure{Format: CSVDataFormat}}},
		// {&Dataset{Abstract: &Dataset{Title: "I'm an abstract dataset"}}},
		{&Dataset{Transform: &Transform{Data: "I'm transform data!"}}},
		{&Dataset{AbstractTransform: &Transform{Data: "I'm abstract transform data?"}}},
		{&Dataset{Commit: &Commit{Title: "foo"}}},
		{&Dataset{DataPath: "foo"}},
		{&Dataset{PreviousPath: "stuff"}},
		{&Dataset{Meta: &Meta{Title: "foo"}}},
		{&Dataset{VisConfig: &VisConfig{Kind: KindVisConfig}}},
	}

	for i, c := range cases {
		got := &Dataset{}
		got.Assign(c.in)
		if err := CompareDatasets(c.in, got); err != nil {
			t.Errorf("case %d error: %s", i, err.Error())
			continue
		}
	}

	// test model assignment
	mads := &Dataset{
		Abstract:          &Dataset{},
		Transform:         &Transform{},
		AbstractTransform: &Transform{},
		Structure:         &Structure{},
		Commit:            &Commit{},
		VisConfig:         &VisConfig{},
	}
	madsa := &Dataset{
		Abstract:          &Dataset{Structure: &Structure{}},
		Transform:         &Transform{Data: "I'm transform data!"},
		AbstractTransform: &Transform{Data: "I'm abstract transform data?"},
		Structure:         &Structure{Format: CSVDataFormat},
		Commit:            &Commit{Title: "dy.no.mite."},
		VisConfig:         &VisConfig{Kind: KindVisConfig},
	}
	mads.Assign(madsa)

	if err := CompareDatasets(mads, madsa); err != nil {
		t.Errorf("error testing assigning to existing substructs: %s", err.Error())
		return
	}
}

func TestDatasetMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Dataset
		out []byte
		err error
	}{
		{&Dataset{}, []byte(`{"qri":"ds:0","structure":null}`), nil},
		{AirportCodes, []byte(AirportCodesJSON), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if !bytes.Equal(c.out, got) {
			t.Errorf("case %d error mismatch. %s != %s", i, string(c.out), string(got))
			continue
		}
	}

	data, err := ioutil.ReadFile("testdata/datasets/complete.json")
	if err != nil {
		t.Errorf("error reading dataset file: %s", err.Error())
		return
	}
	ds := &Dataset{}
	if err := json.Unmarshal(data, &ds); err != nil {
		t.Errorf("error unmarshaling json: %s", err.Error())
		return
	}
	if _, err := ds.MarshalJSON(); err != nil {
		t.Errorf("error marshaling back to json: %s", err.Error())
		return
	}

	strbytes, err := json.Marshal(&Dataset{path: datastore.NewKey("/path/to/dataset")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/dataset\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/dataset\"")
	}
}

func TestDatasetUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Dataset
		err      error
	}{
		{"testdata/datasets/airport-codes.json", AirportCodes, nil},
		{"testdata/datasets/continent-codes.json", ContinentCodes, nil},
		{"testdata/datasets/hours.json", Hours, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Dataset{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err = CompareDatasets(ds, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}
	}

	strds := &Dataset{}
	path := "/path/to/dataset"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strds); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strds.path.String() != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strds.path)
		return
	}
}

func TestDatasetIsEmpty(t *testing.T) {
	cases := []struct {
		ds *Dataset
	}{
		{&Dataset{Abstract: &Dataset{}}},
		{&Dataset{AbstractTransform: &Transform{}}},
		{&Dataset{Commit: &Commit{}}},
		{&Dataset{DataPath: "foo"}},
		{&Dataset{Meta: &Meta{}}},
		{&Dataset{PreviousPath: "nope"}},
		{&Dataset{Structure: &Structure{}}},
		{&Dataset{Transform: &Transform{}}},
		{&Dataset{VisConfig: &VisConfig{}}},
	}

	for i, c := range cases {
		if c.ds.IsEmpty() == true {
			t.Errorf("case %d improperly reported dataset as empty", i)
			continue
		}
	}
}

func TestAbstract(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Dataset
		err      error
	}{
		{"testdata/datasets/airport-codes.json", AirportCodesAbstract, nil},
		// {"testdata/datasets/continent-codes.json", ContinentCodes, nil},
		// {"testdata/datasets/hours.json", Hours, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Dataset{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		abs := Abstract(ds)

		if err = CompareDatasets(abs, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}
	}
}

func TestUnmarshalDataset(t *testing.T) {
	dsa := Dataset{Qri: KindDataset}
	cases := []struct {
		value interface{}
		out   *Dataset
		err   string
	}{
		{dsa, &dsa, ""},
		{&dsa, &dsa, ""},
		{[]byte("{\"qri\":\"ds:0\"}"), &Dataset{Qri: KindDataset}, ""},
		{5, nil, "couldn't parse dataset, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalDataset(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareDatasets(c.out, got); err != nil {
			t.Errorf("case %d dataset mismatch: %s", i, err.Error())
			continue
		}
	}
}
