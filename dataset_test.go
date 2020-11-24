package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestDatasetDropTransientValues(t *testing.T) {
	ds := Dataset{
		Body:        []int{1, 2, 3},
		Name:        "three numbers",
		Peername:    "foo",
		Path:        "/tmp/ds",
		ProfileID:   "QmBlahBlah",
		NumVersions: 4,
	}
	ds.DropTransientValues()
	if !ds.IsEmpty() {
		t.Errorf("error, dropping transient values should leave an empty dataset")
	}

	ds = Dataset{
		Body:        []int{1, 2, 3},
		Name:        "three numbers",
		Peername:    "foo",
		Path:        "/tmp/ds",
		ProfileID:   "QmBlahBlah",
		NumVersions: 4,
		Meta: &Meta{
			Title: "a title",
		},
	}
	ds.DropTransientValues()
	if ds.IsEmpty() {
		t.Errorf("error dataset should not be empty")
	}
}

func TestDatasetDropDerivedValues(t *testing.T) {
	ds := &Dataset{
		Qri:  "definitely qri",
		Path: "/ntwk/QmDsHash",
		Meta: &Meta{
			Qri:  "foo",
			Path: "/ntwk/QmHash",
		},
		Structure: &Structure{
			Checksum: "checksum",
			Depth:    120,
			ErrCount: 4,
			Entries:  1234567890,
			Length:   90210,
			Path:     "/ipfs/QmHash",
			Qri:      "oh you know it's qri",
		},
		Viz: &Viz{
			Qri:  "yep, is qri",
			Path: "/ntwk/QmViz",
		},
		Commit: &Commit{
			Qri:  "qri bird",
			Path: "/ntwk/QmCommit",
		},
		Transform: &Transform{
			Qri:  "qri qri",
			Path: "/ntwk/QmTf",
		},
	}

	ds.DropDerivedValues()

	exp := &Dataset{
		Commit:    &Commit{},
		Meta:      &Meta{},
		Structure: &Structure{},
		Viz:       &Viz{},
		Transform: &Transform{},
	}

	if diff := compareDatasets(ds, exp); diff != "" {
		t.Logf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestDatasetOpenBodyFile(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestDatasetAssign(t *testing.T) {
	// TODO - expand test to check all fields
	cases := []struct {
		in *Dataset
	}{
		{&Dataset{Path: "/a"}},
		{&Dataset{Structure: &Structure{Format: "csv"}}},
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

		if diff := compareDatasets(c.in, got); diff != "" {
			t.Errorf("case %d result mismatch. (-want +got):\n%s", i, diff)
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
		Structure: &Structure{Format: "csv"},
		Commit:    &Commit{Title: "dy.no.mite."},
		Viz:       &Viz{Qri: KindViz.String()},
	}
	mads.Assign(madsa)

	if diff := compareDatasets(mads, madsa); diff != "" {
		t.Errorf("error testing assigning to existing substructs (-want +got):\n%s", diff)
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

func TestSigningBytes(t *testing.T) {
	ds := &Dataset{
		Commit:    &Commit{Timestamp: time.Date(2001, 1, 1, 1, 1, 1, 1, time.UTC)},
		BodyPath:  "body",
		Meta:      &Meta{Path: "meta"},
		Readme:    &Readme{Path: "readme"},
		Structure: &Structure{Path: "structure"},
		Transform: &Transform{Path: "transform"},
		Stats:     &Stats{Path: "stats"},
		Viz:       &Viz{Path: "viz"},
	}

	got := ds.SigningBytes()

	expect := `bd:body
cm:2001-01-01T01:01:01Z
md:meta
rm:readme
st:structure
tf:transform
sa:stats
vz:viz`

	if diff := cmp.Diff(expect, string(got)); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestDatasetMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Dataset
		out []byte
		err error
	}{
		{&Dataset{}, []byte(`{"qri":"ds:0"}`), nil},
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

		if diff := compareDatasets(ds, c.result); diff != "" {
			t.Errorf("case %d resource comparison error:(-want +got):\n%s", i, diff)
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
		{&Dataset{Stats: &Stats{}}},
		{&Dataset{Viz: &Viz{}}},
	}

	for i, c := range cases {
		if c.ds.IsEmpty() == true {
			t.Errorf("case %d improperly reported dataset as empty", i)
			continue
		}
	}
}

func TestPathMap(t *testing.T) {
	cases := []struct {
		Ds         *Dataset
		Ignore     []string
		ExpectJSON string
	}{
		{nil, nil, `{}`},
		{
			&Dataset{Path: "ds"},
			nil,
			`{ "dataset": "ds"}`,
		},
		{
			&Dataset{
				Path:      "ds",
				BodyPath:  "bd",
				Commit:    &Commit{Path: "cm"},
				Transform: &Transform{Path: "tf"},
				Structure: &Structure{Path: "st"},
				Stats:     &Stats{Path: "sa"},
				Meta:      &Meta{Path: "md"},
				Readme:    &Readme{Path: "rm"},
				Viz:       &Viz{Path: "vz"},
			},
			nil,
			`{ 
				"dataset": "ds", 
				"body": "bd", 
				"commit": "cm",
				"transform": "tf",
				"structure": "st",
				"stats": "sa",
				"meta": "md",
				"readme": "rm",
				"viz": "vz"
			}`,
		},
		{
			&Dataset{
				Path:      "ds",
				BodyPath:  "bd",
				Commit:    &Commit{Path: "cm"},
				Transform: &Transform{Path: "tf"},
				Structure: &Structure{Path: "st"},
				Stats:     &Stats{Path: "sa"},
				Meta:      &Meta{Path: "md"},
				Readme:    &Readme{Path: "rm"},
				Viz:       &Viz{Path: "vz"},
			},
			[]string{
				"dataset",
				"body",
				"commit",
				"meta",
				"transform",
				"structure",
				"stats",
				"readme",
				"viz",
			},
			`{}`,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			expect := mustJSONMap(c.ExpectJSON)
			got := c.Ds.PathMap(c.Ignore...)
			if diff := cmp.Diff(expect, got); diff != "" {
				t.Errorf("result mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func mustJSONMap(data string) (out map[string]string) {
	out = map[string]string{}
	if err := json.Unmarshal([]byte(data), &out); err != nil {
		panic(err)
	}
	return out
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
		if diff := compareDatasets(c.out, got); diff != "" {
			t.Errorf("case %d dataset mismatch (-want +got):\n%s", i, diff)
			continue
		}
	}
}

func compareDatasets(a, b *Dataset) string {
	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(
		Dataset{},
		Meta{},
		Transform{},
		Readme{},
		Viz{},
	))
}
