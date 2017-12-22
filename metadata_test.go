package dataset

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
)

func TestMetadataAssign(t *testing.T) {
	// TODO - expand test to check all fields
	cases := []struct {
		in *Metadata
	}{
		{&Metadata{path: datastore.NewKey("/a")}},
		{&Metadata{AccessPath: "foo"}},
		{&Metadata{DownloadPath: "foo"}},
		{&Metadata{ReadmePath: "foo"}},
		{&Metadata{AccrualPeriodicity: "1W"}},
		{&Metadata{Citations: []*Citation{&Citation{Email: "foo"}}}},
		{&Metadata{Description: "foo"}},
		{&Metadata{HomePath: "foo"}},
		{&Metadata{Identifier: "foo"}},
		{&Metadata{License: &License{Type: "foo"}}},
		{&Metadata{Version: "foo"}},
		{&Metadata{Keywords: []string{"foo"}}},
		{&Metadata{Contributors: []*User{&User{Email: "foo"}}}},
		{&Metadata{Language: []string{"stuff"}}},
		{&Metadata{Theme: []string{"stuff"}}},
		{&Metadata{meta: map[string]interface{}{"foo": "bar"}}},
	}

	for i, c := range cases {
		got := &Metadata{}
		got.Assign(c.in)
		if err := CompareMetadatas(c.in, got); err != nil {
			t.Errorf("case %d error: %s", i, err.Error())
			continue
		}
	}

	expect := &Metadata{
		Title:       "Final Title",
		Description: "Final Description",
		AccessPath:  "AccessPath",
	}
	got := &Metadata{
		Title:       "Overwrite Me",
		Description: "Nope",
	}

	got.Assign(&Metadata{
		Title:       "Final Title",
		Description: "Final Description",
		AccessPath:  "AccessPath",
	})

	if err := CompareMetadatas(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareMetadatas(expect, got); err != nil {
		t.Error(err)
	}

	emptyDs := &Metadata{}
	emptyDs.Assign(expect)
	if err := CompareMetadatas(expect, emptyDs); err != nil {
		t.Error(err)
	}
}

func TestMetadataMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Metadata
		out []byte
		err error
	}{
		{&Metadata{}, []byte(`{"kind":"qri:md:0"}`), nil},
		{AirportCodes.Metadata, []byte(`{"citations":[{"name":"Our Airports","url":"http://ourairports.com/data/"}],"homePath":"http://www.ourairports.com/","kind":"qri:md:0","license":"PDDL-1.0","title":"Airport Codes"}`), nil},
		{Hours.Metadata, []byte(`{"accessPath":"https://example.com/not/a/url","downloadPath":"https://example.com/not/a/url","kind":"qri:md:0","readmePath":"/ipfs/notahash","title":"hours"}`), nil},
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
	ds := &Metadata{}
	if err := json.Unmarshal(data, &ds); err != nil {
		t.Errorf("error unmarshaling json: %s", err.Error())
		return
	}
	if _, err := ds.MarshalJSON(); err != nil {
		t.Errorf("error marshaling back to json: %s", err.Error())
		return
	}

	strbytes, err := json.Marshal(&Metadata{path: datastore.NewKey("/path/to/dataset")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/dataset\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/dataset\"")
	}
}

func TestMetadataUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Metadata
		err      error
	}{
		{"testdata/metadata/airport-codes.json", AirportCodes.Metadata, nil},
		{"testdata/metadata/continent-codes.json", ContinentCodes.Metadata, nil},
		{"testdata/metadata/hours.json", Hours.Metadata, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Metadata{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err = CompareMetadatas(ds, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}
	}

	strds := &Metadata{}
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

func TestUnmarshalMetadata(t *testing.T) {
	dsa := Metadata{Kind: KindMetadata}
	cases := []struct {
		value interface{}
		out   *Metadata
		err   string
	}{
		{dsa, &dsa, ""},
		{&dsa, &dsa, ""},
		{[]byte("{\"kind\":\"qri:md:0\"}"), &Metadata{Kind: KindMetadata}, ""},
		{5, nil, "couldn't parse metadata, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalMetadata(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareMetadatas(c.out, got); err != nil {
			t.Errorf("case %d metadata mismatch: %s", i, err.Error())
			continue
		}
	}
}

func TestLicense(t *testing.T) {

}

func TestAccrualDuration(t *testing.T) {
	cases := []struct {
		in     string
		expect time.Duration
	}{
		{"", time.Duration(0)},
		{"R/P10Y", time.Duration(315360000000000000)},
		{"R/P4Y", time.Duration(126144000000000000)},
		{"R/P1Y", time.Duration(31536000000000000)},
		{"R/P2M", time.Duration(25920000000000000)},
		{"R/P3.5D", time.Duration(345600000000000)},
		{"R/P1D", time.Duration(86400000000000)},
		{"R/P2W", time.Duration(1209600000000000)},
		{"R/P6M", time.Duration(15552000000000000)},
		{"R/P2Y", time.Duration(63072000000000000)},
		{"R/P3Y", time.Duration(94608000000000000)},
		{"R/P0.33W", time.Duration(201600000000000)},
		{"R/P0.33M", time.Duration(864000000000000)},
		{"R/PT1S", time.Duration(1000000000)},
		{"R/P1M", time.Duration(2592000000000000)},
		{"R/P3M", time.Duration(4505142857142857)},
		{"R/P0.5M", time.Duration(1296000000000000)},
		{"R/P4M", time.Duration(7884000000000000)},
		{"R/P1W", time.Duration(604800000000000)},
		{"R/PT1H", time.Duration(3600000000000)},
	}

	for i, c := range cases {
		got := AccuralDuration(c.in)
		if got != c.expect {
			t.Errorf("case %d error. expected: %d, got: %d", i, c.expect, got)
		}
	}
}
