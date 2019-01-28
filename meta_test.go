package dataset

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"
)

func TestMetaSetPath(t *testing.T) {
	cases := []struct {
		path   string
		expect *Meta
	}{
		{"", &Meta{}},
		{"path", &Meta{path: "path"}},
	}

	for i, c := range cases {
		got := &Meta{}
		got.SetPath(c.path)
		if err := CompareMetas(c.expect, got); err != nil {
			t.Errorf("case %d error: %s", i, err)
			continue
		}
	}
}

func TestMetaAssign(t *testing.T) {
	// TODO - expand test to check all fields
	cases := []struct {
		in *Meta
	}{
		{&Meta{path: "/a"}},
		{&Meta{AccessURL: "foo"}},
		{&Meta{DownloadURL: "foo"}},
		{&Meta{ReadmeURL: "foo"}},
		{&Meta{AccrualPeriodicity: "1W"}},
		{&Meta{Citations: []*Citation{{Email: "foo"}}}},
		{&Meta{Description: "foo"}},
		{&Meta{HomeURL: "foo"}},
		{&Meta{Identifier: "foo"}},
		{&Meta{License: &License{Type: "foo"}}},
		{&Meta{Version: "foo"}},
		{&Meta{Keywords: []string{"foo"}}},
		{&Meta{Contributors: []*User{{Email: "foo"}}}},
		{&Meta{Language: []string{"stuff"}}},
		{&Meta{Theme: []string{"stuff"}}},
		{&Meta{meta: map[string]interface{}{"foo": "bar"}}},
	}

	for i, c := range cases {
		got := &Meta{}
		got.Assign(c.in)
		if err := CompareMetas(c.in, got); err != nil {
			t.Errorf("case %d error: %s", i, err.Error())
			continue
		}
	}

	expect := &Meta{
		Title:       "Final Title",
		Description: "Final Description",
		AccessURL:   "AccessURL",
	}
	got := &Meta{
		Title:       "Overwrite Me",
		Description: "Nope",
	}

	got.Assign(&Meta{
		Title:       "Final Title",
		Description: "Final Description",
		AccessURL:   "AccessURL",
	})

	if err := CompareMetas(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareMetas(expect, got); err != nil {
		t.Error(err)
	}

	emptyDs := &Meta{}
	emptyDs.Assign(expect)
	if err := CompareMetas(expect, emptyDs); err != nil {
		t.Error(err)
	}
}

func TestMetaSet(t *testing.T) {
	cases := []struct {
		key  string
		val  interface{}
		err  string
		meta *Meta
	}{
		{" TITLE  ", 0, "type must be a string", nil},
		{" TITLE", "title", "", &Meta{Title: "title"}},
		{" TITLE", nil, "", &Meta{}},
		{"accessurl", 0, "type must be a string", nil},
		{"accessurl", "foo", "", &Meta{AccessURL: "foo"}},
		{"accrualperiodicity", 0, "type must be a string", nil},
		{"accrualperiodicity", "foo", "", &Meta{AccrualPeriodicity: "foo"}},
		{"description", 0, "type must be a string", nil},
		{"description", "foo", "", &Meta{Description: "foo"}},
		{"downloadurl", 0, "type must be a string", nil},
		{"downloadurl", "foo", "", &Meta{DownloadURL: "foo"}},
		{"homeurl", 0, "type must be a string", nil},
		{"homeurl", "foo", "", &Meta{HomeURL: "foo"}},
		{"identifier", 0, "type must be a string", nil},
		{"identifier", "foo", "", &Meta{Identifier: "foo"}},
		{"readmeurl", 0, "type must be a string", nil},
		{"readmeurl", "foo", "", &Meta{ReadmeURL: "foo"}},
		{"title", 0, "type must be a string", nil},
		{"title", "foo", "", &Meta{Title: "foo"}},
		{"version", 0, "type must be a string", nil},
		{"version", "foo", "", &Meta{Version: "foo"}},

		{"keywords", 0, "type must be a set of strings", nil},
		{"keywords", nil, "", &Meta{}},
		{"keywords", []interface{}{0}, "index 0: type must be a string", nil},
		{"keywords", []interface{}{"foo"}, "", &Meta{Keywords: []string{"foo"}}},
		{"language", 0, "type must be a set of strings", nil},
		{"language", []interface{}{"foo"}, "", &Meta{Language: []string{"foo"}}},
		{"theme", 0, "type must be a set of strings", nil},
		{"theme", []interface{}{"foo"}, "", &Meta{Theme: []string{"foo"}}},

		{"citations", 0, "citation: expected interface slice", nil},
		{"citations", []interface{}{0}, "parsing citations index 0: expected map[string]interface{}", nil},
		{"citations", []interface{}{
			map[string]interface{}{
				"name": "foo",
				"url":  "bar",
			},
		}, "", &Meta{Citations: []*Citation{&Citation{Name: "foo", URL: "bar"}}}},

		{"contributors", 0, "contributors: expected interface slice", nil},
		{"contributors", []interface{}{0}, "parsing contributors index 0: expected map[string]interface{}", nil},
		{"contributors", []interface{}{
			map[string]interface{}{
				"id":    "steve",
				"email": "email@steve.com",
			}}, "", &Meta{Contributors: []*User{&User{ID: "steve", Email: "email@steve.com"}}}},

		{"license", 0, "expected map[string]interface{}", nil},
		{"license", map[string]interface{}{
			"type": "foo",
			"url":  "bar",
		}, "", &Meta{License: &License{Type: "foo", URL: "bar"}}},

		{"@id", "foo", "", &Meta{meta: map[string]interface{}{"@id": "foo"}}},
	}

	for i, c := range cases {
		m := &Meta{}
		err := m.Set(c.key, c.val)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d (%s) error mismatch. expected: '%s', got: '%s'", i, c.key, c.err, err)
			continue
		}
		if c.meta != nil {
			if err := CompareMetas(m, c.meta); err != nil {
				t.Errorf("case %d (%s) meta mismatch: %s", i, c.key, err.Error())
				continue
			}
		}
	}
}

func TestMetaMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Meta
		out []byte
		err error
	}{
		{&Meta{}, []byte(`{"qri":"md:0"}`), nil},
		{AirportCodes.Meta, []byte(`{"citations":[{"name":"Our Airports","url":"http://ourairports.com/data/"}],"homeURL":"http://www.ourairports.com/","license":{"type":"PDDL-1.0"},"qri":"md:0","title":"Airport Codes"}`), nil},
		{Hours.Meta, []byte(`{"accessURL":"https://example.com/not/a/url","downloadURL":"https://example.com/not/a/url","qri":"md:0","readmeURL":"/ipfs/notahash","title":"hours"}`), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if !bytes.Equal(c.out, got) {
			t.Errorf("case %d mismatch. expected != got: %s != %s", i, string(c.out), string(got))
			continue
		}
	}

	data, err := ioutil.ReadFile("testdata/datasets/complete.json")
	if err != nil {
		t.Errorf("error reading dataset file: %s", err.Error())
		return
	}
	ds := &Meta{}
	if err := json.Unmarshal(data, &ds); err != nil {
		t.Errorf("error unmarshaling json: %s", err.Error())
		return
	}
	if _, err := ds.MarshalJSON(); err != nil {
		t.Errorf("error marshaling back to json: %s", err.Error())
		return
	}

	strbytes, err := json.Marshal(&Meta{path: "/path/to/dataset"})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/dataset\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/dataset\"")
	}
}

func TestMetaUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Meta
		err      error
	}{
		{"testdata/metadata/airport-codes.json", AirportCodes.Meta, nil},
		{"testdata/metadata/continent-codes.json", ContinentCodes.Meta, nil},
		{"testdata/metadata/hours.json", Hours.Meta, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Meta{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d error mismatch (expected: '%s', got): '%s'", i, c.err, err)
			continue
		}

		if err = CompareMetas(ds, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}
	}

	strds := &Meta{}
	path := "/path/to/dataset"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strds); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strds.path != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strds.path)
		return
	}
}

func TestUnmarshalMeta(t *testing.T) {
	dsa := Meta{Qri: KindMeta}
	cases := []struct {
		value interface{}
		out   *Meta
		err   string
	}{
		{dsa, &dsa, ""},
		{&dsa, &dsa, ""},
		{[]byte("{\"qri\":\"md:0\"}"), &Meta{Qri: KindMeta}, ""},
		{5, nil, "couldn't parse metadata, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalMeta(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareMetas(c.out, got); err != nil {
			t.Errorf("case %d metadata mismatch: %s", i, err.Error())
			continue
		}
	}
}

func TestCitationDecode(t *testing.T) {
	c := &Citation{}
	if err := c.Decode(map[string]interface{}{"name": 0}); err == nil {
		t.Errorf("expected error")
	}
	if err := c.Decode(map[string]interface{}{"url": 0}); err == nil {
		t.Errorf("expected error")
	}
	if err := c.Decode(map[string]interface{}{"email": 0}); err == nil {
		t.Errorf("expected error")
	}
}

func TestLicenseDecode(t *testing.T) {
	l := &License{}
	if err := l.Decode(map[string]interface{}{"type": 0}); err == nil {
		t.Errorf("expected error")
	}
	if err := l.Decode(map[string]interface{}{"url": 0}); err == nil {
		t.Errorf("expected error")
	}
}

func TestUserDecode(t *testing.T) {
	u := &User{}
	if err := u.Decode(map[string]interface{}{"id": 0}); err == nil {
		t.Errorf("expected error")
	}
	if err := u.Decode(map[string]interface{}{"name": 0}); err == nil {
		t.Errorf("expected error")
	}
	if err := u.Decode(map[string]interface{}{"email": 0}); err == nil {
		t.Errorf("expected error")
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
