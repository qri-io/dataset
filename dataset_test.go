package dataset

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset/datatypes"
)

func TestDatasetAssign(t *testing.T) {
	// TODO - expand test to check all fields
	cases := []struct {
		in *Dataset
	}{
		{&Dataset{path: datastore.NewKey("/a")}},
		{&Dataset{Timestamp: time.Now()}},
		{&Dataset{Structure: &Structure{Format: CSVDataFormat}}},
		{&Dataset{Abstract: &Dataset{Title: "I'm an abstract dataset"}}},
		{&Dataset{Transform: &Transform{Data: "I'm transform data!"}}},
		{&Dataset{AbstractTransform: &Transform{Data: "I'm abstract transform data?"}}},
		{&Dataset{Commit: &CommitMsg{Title: "foo"}}},
		{&Dataset{Data: "foo"}},
		{&Dataset{Length: 2503}},
		{&Dataset{AccessURL: "foo"}},
		{&Dataset{DownloadURL: "foo"}},
		{&Dataset{Readme: "foo"}},
		{&Dataset{Author: &User{Email: "foo"}}},
		{&Dataset{AccrualPeriodicity: "1W"}},
		{&Dataset{Citations: []*Citation{&Citation{Email: "foo"}}}},
		{&Dataset{Image: "foo"}},
		{&Dataset{Description: "foo"}},
		{&Dataset{Homepage: "foo"}},
		{&Dataset{IconImage: "foo"}},
		{&Dataset{Identifier: "foo"}},
		{&Dataset{License: &License{Type: "foo"}}},
		{&Dataset{Version: "foo"}},
		{&Dataset{Keywords: []string{"foo"}}},
		{&Dataset{Contributors: []*User{&User{Email: "foo"}}}},
		{&Dataset{Language: []string{"stuff"}}},
		{&Dataset{Theme: []string{"stuff"}}},
		{&Dataset{QueryString: "stuff"}},
		{&Dataset{Previous: datastore.NewKey("stuff")}},
		{&Dataset{meta: map[string]interface{}{"foo": "bar"}}},
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
		Commit:            &CommitMsg{},
	}
	madsa := &Dataset{
		Abstract:          &Dataset{Title: "I'm an abstract dataset"},
		Transform:         &Transform{Data: "I'm transform data!"},
		AbstractTransform: &Transform{Data: "I'm abstract transform data?"},
		Structure:         &Structure{Format: CSVDataFormat},
		Commit:            &CommitMsg{Title: "dy.no.mite."},
	}
	mads.Assign(madsa)

	if err := CompareDatasets(mads, madsa); err != nil {
		t.Errorf("error testing assigning to existing substructs: %s", err.Error())
		return
	}

	expect := &Dataset{
		Title:       "Final Title",
		Description: "Final Description",
		AccessURL:   "AccessURL",
		Structure: &Structure{
			Schema: &Schema{
				Fields: []*Field{
					{Type: datatypes.String, Name: "foo"},
					{Type: datatypes.Integer, Name: "bar"},
					{Description: "bat"},
				},
			},
		},
	}
	got := &Dataset{
		Title:       "Overwrite Me",
		Description: "Nope",
		Structure: &Structure{
			Schema: &Schema{
				Fields: []*Field{
					{Type: datatypes.String},
					{Type: datatypes.Integer},
				},
			},
		},
	}

	got.Assign(&Dataset{
		Title:       "Final Title",
		Description: "Final Description",
		AccessURL:   "AccessURL",
		Structure: &Structure{
			Schema: &Schema{
				Fields: []*Field{
					{Name: "foo"},
					{Name: "bar"},
					{Description: "bat"},
				},
			},
		},
	})

	if err := CompareDatasets(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareDatasets(expect, got); err != nil {
		t.Error(err)
	}

	emptyDs := &Dataset{}
	emptyDs.Assign(expect)
	if err := CompareDatasets(expect, emptyDs); err != nil {
		t.Error(err)
	}
}

func TestDatasetMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Dataset
		out []byte
		err error
	}{
		{&Dataset{}, []byte(`{"kind":"qri:ds:0","structure":null}`), nil},
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
	dsa := Dataset{Kind: KindDataset, Title: "foo"}
	cases := []struct {
		value interface{}
		out   *Dataset
		err   string
	}{
		{dsa, &dsa, ""},
		{&dsa, &dsa, ""},
		{[]byte("{\"kind\":\"qri:ds:0\"}"), &Dataset{Kind: KindDataset}, ""},
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
