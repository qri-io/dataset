package dataset

import (
	"bytes"
	"encoding/json"
	"github.com/ipfs/go-datastore"
	"io/ioutil"
	"testing"
)

var Viz1 = &Viz{
	Format:     "foo",
	Qri:        KindViz,
	ScriptPath: "one",
}

var Viz2 = &Viz{
	Format:     "bar",
	Qri:        KindViz,
	ScriptPath: "two",
}

var Viz3 = &Viz{
	Format:     "bar",
	Qri:        KindViz,
	ScriptPath: "three",
}

func TestVizSetPath(t *testing.T) {
	cases := []struct {
		path   string
		expect *Viz
	}{
		{"", &Viz{path: datastore.Key{}}},
		{"path", &Viz{path: datastore.NewKey("path")}},
	}

	for i, c := range cases {
		got := &Viz{}
		got.SetPath(c.path)
		if err := CompareVizs(c.expect, got); err != nil {
			t.Errorf("case %d error: %s", i, err)
			continue
		}
	}
}

func TestVizAssign(t *testing.T) {
	cases := []struct {
		got    *Viz
		assign *Viz
		expect *Viz
		err    string
	}{
		{nil, nil, nil, ""},
		{&Viz{}, Viz1, Viz1, ""},
		{&Viz{
			Format:     "bar",
			Qri:        KindViz,
			ScriptPath: "replace me",
		},
			Viz2, Viz2, ""},
		{&Viz{
			Format:     "bar",
			Qri:        KindViz,
			ScriptPath: "replace me",
		},
			Viz2, Viz3, "ScriptPath: three != two"},
		{&Viz{
			path:       datastore.NewKey("foo"),
			Format:     "foo",
			Qri:        KindViz,
			ScriptPath: "bat",
		},
			&Viz{path: datastore.NewKey("bar"), Format: "bar"},
			&Viz{
				path:       datastore.NewKey("foo"),
				Format:     "bar",
				Qri:        KindViz,
				ScriptPath: "bat",
			}, ""},
	}
	for i, c := range cases {
		c.got.Assign(c.assign)
		err := CompareVizs(c.expect, c.got)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
		}
	}

}

func TestVizIsEmpty(t *testing.T) {
	cases := []struct {
		vc       *Viz
		expected bool
	}{
		{&Viz{Qri: KindViz}, true},
		{&Viz{ScriptPath: "foo"}, false},
		{&Viz{}, true},
		{&Viz{path: datastore.NewKey("foo")}, true},
	}

	for i, c := range cases {
		if c.vc.IsEmpty() != c.expected {
			t.Errorf("case %d improperly reported visconfig as empty == %v", i, c.expected)
			continue
		}
	}
}

func TestVizUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Viz
		err      string
	}{
		{"testdata/vizs/invalidJSON.json", nil, `invalid character 'I' looking for beginning of value`},
		{"testdata/vizs/visconfig1.json", Viz1, ""},
		{"testdata/vizs/visconfig2.json", Viz2, ""},
		{"testdata/vizs/visconfig3.json", Viz3, ""},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		vc := &Viz{}
		err = json.Unmarshal(data, vc)
		if err != nil {
			if err.Error() != c.err {
				t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
				continue
			} else {
				continue
			}
		}

		if err = CompareVizs(vc, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}
	}

	vc := &Viz{}
	path := "/path/to/visconfig"
	if err := json.Unmarshal([]byte(`"`+path+`"`), vc); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if vc.path.String() != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, vc.path)
		return
	}
}

func TestVizMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Viz
		out []byte
		err string
	}{
		{&Viz{}, []byte(`{"qri":"vz:0"}`), ""},
		{&Viz{Qri: KindViz}, []byte(`{"qri":"vz:0"}`), ""},
		{&Viz{Format: "foo", Qri: KindViz}, []byte(`{"format":"foo","qri":"vz:0"}`), ""},
		{Viz1, []byte(`{"format":"foo","qri":"vz:0","scriptPath":"one"}`), ""},
		{&Viz{path: datastore.NewKey("/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U")}, []byte(`"/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U"`), ""},
		{&Viz{path: datastore.NewKey("/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD")}, []byte(`"/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"`), ""},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if string(c.out) != string(got) {
			t.Errorf("case %d, %s != %s", i, string(c.out), string(got))
			continue
		}
	}

	vcbytes, err := json.Marshal(&Viz{path: datastore.NewKey("/path/to/Viz")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(vcbytes, []byte("\"/path/to/Viz\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(vcbytes), "\"/path/to/Viz\"")
	}
}

func TestVizMarshalJSONObject(t *testing.T) {
	cases := []struct {
		in  *Viz
		out []byte
		err string
	}{
		{&Viz{}, []byte(`{"qri":"vz:0"}`), ""},
		{&Viz{Qri: KindViz}, []byte(`{"qri":"vz:0"}`), ""},
		{&Viz{Format: "foo", Qri: KindViz}, []byte(`{"format":"foo","qri":"vz:0"}`), ""},
		{Viz1, []byte(`{"format":"foo","qri":"vz:0","visualizations":{"colors":{"background":"#000000","bars":"#ffffff"},"type":"bar"}}`), ""},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		check := &map[string]interface{}{}
		err = json.Unmarshal(got, check)
		if err != nil {
			t.Errorf("case %d error: failed to unmarshal to object: %s", i, err.Error())
			continue
		}

	}
}

func TestUnmarshalViz(t *testing.T) {
	vc := Viz{Qri: KindViz, Format: "foo"}
	cases := []struct {
		value interface{}
		out   *Viz
		err   string
	}{
		{vc, &vc, ""},
		{&vc, &vc, ""},
		{[]byte("{\"qri\":\"vz:0\"}"), &Viz{Qri: KindViz}, ""},
		{5, nil, "couldn't parse Viz, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalViz(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareVizs(c.out, got); err != nil {
			t.Errorf("case %d Viz mismatch: %s", i, err.Error())
			continue
		}
	}
}
