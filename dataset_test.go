package dataset

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"
)

func TestDatasetAssign(t *testing.T) {
	// TODO - expand test to check all fields
	cases := []struct {
		in *Dataset
	}{
		{&Dataset{Path: "/a"}},
		{&Dataset{Structure: &Structure{Format: CSVDataFormat}}},
		{&Dataset{Transform: &Transform{ScriptPath: "some_transform_script.star"}}},
		{&Dataset{Commit: &Commit{Title: "foo"}}},
		{&Dataset{BodyPath: "foo"}},
		{&Dataset{PreviousPath: "stuff"}},
		{&Dataset{Meta: &Meta{Title: "foo"}}},
		{&Dataset{Viz: &Viz{Qri: KindViz.String()}}},
	}

	for i, c := range cases {
		got := &Dataset{}
		got.Assign(c.in)
		// assign resets the path:
		if err := CompareDatasets(c.in, got); err != nil {
			t.Errorf("case %d error: %s", i, err.Error())
			continue
		}
	}

	// test model assignment
	mads := &Dataset{
		Transform: &Transform{},
		Structure: &Structure{},
		Commit:    &Commit{},
		Viz:       &Viz{},
	}
	madsa := &Dataset{
		Transform: &Transform{ScriptPath: "some_transform_script.star"},
		Structure: &Structure{Format: CSVDataFormat},
		Commit:    &Commit{Title: "dy.no.mite."},
		Viz:       &Viz{Qri: KindViz.String()},
	}
	mads.Assign(madsa)

	if err := CompareDatasets(mads, madsa); err != nil {
		t.Errorf("error testing assigning to existing substructs: %s", err.Error())
		return
	}
}

func TestDatasetSignableBytes(t *testing.T) {
	loc, err := time.LoadLocation("America/Toronto")
	if err != nil {
		t.Errorf("error getting timezone: %s", err.Error())
		return
	}

	cases := []struct {
		ds     *Dataset
		expect []byte
		err    string
	}{
		{&Dataset{}, nil, "commit is required"},
		{&Dataset{Commit: &Commit{}}, nil, "structure is required"},
		{&Dataset{Commit: &Commit{}, Structure: &Structure{}}, []byte("0001-01-01T00:00:00Z\n"), ""},
		{&Dataset{Commit: &Commit{Timestamp: time.Date(2001, 01, 01, 01, 01, 01, 0, time.UTC)}, Structure: &Structure{Checksum: "checksum"}}, []byte("2001-01-01T01:01:01Z\nchecksum"), ""},
		{&Dataset{Commit: &Commit{Timestamp: time.Date(2001, 01, 01, 01, 01, 01, 0, loc)}, Structure: &Structure{Checksum: "checksum"}}, []byte("2001-01-01T06:01:01Z\nchecksum"), ""},
	}

	for i, c := range cases {
		got, err := c.ds.SignableBytes()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if c.err == "" && !bytes.Equal(got, c.expect) {
			t.Errorf("case %d result mismatch. expected: '%s'\n got: '%s'", i, string(c.expect), string(got))
		}
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
			t.Errorf("case %d mismatch. (expected != got) %s != %s", i, string(c.out), string(got))
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

	strbytes, err := json.Marshal(&Dataset{Path: "/path/to/dataset"})
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
		err      string
	}{
		{"testdata/datasets/airport-codes.json", AirportCodes, ""},
		{"testdata/datasets/continent-codes.json", ContinentCodes, ""},
		{"testdata/datasets/hours.json", Hours, ""},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Dataset{}
		err = ds.UnmarshalJSON(data)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
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

	if strds.Path != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strds.Path)
		return
	}

	errDs := &Dataset{}
	if err := errDs.UnmarshalJSON([]byte(`{{{{`)); err != nil && err.Error() != "unmarshaling dataset: invalid character '{' looking for beginning of object key string" {
		t.Errorf("unexpected error: %s", err.Error())
	} else if err == nil {
		t.Errorf("expected error")
	}
}

func TestDatasetIsEmpty(t *testing.T) {
	cases := []struct {
		ds *Dataset
	}{
		{&Dataset{Commit: &Commit{}}},
		{&Dataset{BodyPath: "foo"}},
		{&Dataset{Meta: &Meta{}}},
		{&Dataset{PreviousPath: "nope"}},
		{&Dataset{Structure: &Structure{}}},
		{&Dataset{Transform: &Transform{}}},
		{&Dataset{Viz: &Viz{}}},
	}

	for i, c := range cases {
		if c.ds.IsEmpty() == true {
			t.Errorf("case %d improperly reported dataset as empty", i)
			continue
		}
	}
}

func TestUnmarshalDataset(t *testing.T) {
	dsa := Dataset{Qri: KindDataset.String()}
	cases := []struct {
		value interface{}
		out   *Dataset
		err   string
	}{
		{dsa, &dsa, ""},
		{&dsa, &dsa, ""},
		{[]byte("{\"qri\":\"ds:0\"}"), &Dataset{Qri: KindDataset.String()}, ""},
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
