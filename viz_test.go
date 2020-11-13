package dataset

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func compareVizs(a, b *Viz) string {
	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(Viz{}))
}

func TestVizDropTransientValues(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestVizDropDerivedValues(t *testing.T) {
	vz := &Viz{
		Path: "/ipfs/QmHash",
		Qri:  "oh you know it's qri",
	}

	vz.DropDerivedValues()

	if diff := compareVizs(vz, &Viz{}); diff != "" {
		t.Errorf("expected dropping a viz of only derived values to be empty. diff (-want +got):\n%s", diff)
	}
}

func TestVizScript(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestVizOpenScriptFile(t *testing.T) {
	t.Log("TODO (b5)")
}

var viz1 = &Viz{
	Format:       "foo",
	Qri:          KindViz.String(),
	ScriptPath:   "one",
	RenderedPath: "one",
}

var viz2 = &Viz{
	Format:     "bar",
	Qri:        KindViz.String(),
	ScriptPath: "two",
}

var viz3 = &Viz{
	Format:     "bar",
	Qri:        KindViz.String(),
	ScriptPath: "three",
}

func TestVizAssign(t *testing.T) {
	cases := []struct {
		got    *Viz
		assign *Viz
		expect *Viz
	}{
		{nil, nil, nil},
		{&Viz{}, viz1, viz1},
		{&Viz{
			Format:     "bar",
			Qri:        KindViz.String(),
			ScriptPath: "replace me",
		},
			viz2, viz2},
		// {&Viz{
		// 	Format:     "bar",
		// 	Qri:        KindViz.String(),
		// 	ScriptPath: "replace me",
		// },
		// 	viz2, viz3, "ScriptPath: three != two"},
		{&Viz{
			Path:       "foo",
			Format:     "foo",
			Qri:        KindViz.String(),
			ScriptPath: "bat",
		},
			&Viz{Path: "bar", Format: "bar", RenderedPath: "rendered"},
			&Viz{
				Path:         "bar",
				Format:       "bar",
				Qri:          KindViz.String(),
				ScriptPath:   "bat",
				RenderedPath: "rendered",
			}},
	}
	for i, c := range cases {
		c.got.Assign(c.assign)
		if diff := compareVizs(c.expect, c.got); diff != "" {
			t.Errorf("case %d result mismatch. (-want +got):\n%s", i, diff)
		}
	}

}

func TestVizIsEmpty(t *testing.T) {
	cases := []struct {
		vc       *Viz
		expected bool
	}{
		{&Viz{Qri: KindViz.String()}, true},
		{&Viz{ScriptPath: "foo"}, false},
		{&Viz{RenderedPath: "foo"}, false},
		{&Viz{}, true},
		{&Viz{Path: "foo"}, true},
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
		{"testdata/vizs/visconfig1.json", viz1, ""},
		{"testdata/vizs/visconfig2.json", viz2, ""},
		{"testdata/vizs/visconfig3.json", viz3, ""},
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

		if diff := compareVizs(vc, c.result); diff != "" {
			t.Errorf("case %d resource comparison error (-want +got):\n%s", i, diff)
			continue
		}
	}

	vc := &Viz{}
	path := "/path/to/visconfig"
	if err := json.Unmarshal([]byte(`"`+path+`"`), vc); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if vc.Path != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, vc.Path)
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
		{&Viz{Qri: KindViz.String()}, []byte(`{"qri":"vz:0"}`), ""},
		{&Viz{Format: "foo", Qri: KindViz.String()}, []byte(`{"format":"foo","qri":"vz:0"}`), ""},
		{viz1, []byte(`{"format":"foo","qri":"vz:0","renderedPath":"one","scriptPath":"one"}`), ""},
		{&Viz{Path: "/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U"}, []byte(`"/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U"`), ""},
		{&Viz{Path: "/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"}, []byte(`"/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"`), ""},
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

	vcbytes, err := json.Marshal(&Viz{Path: "/path/to/Viz"})
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
		{&Viz{Qri: KindViz.String()}, []byte(`{"qri":"vz:0"}`), ""},
		{&Viz{Format: "foo", Qri: KindViz.String()}, []byte(`{"format":"foo","qri":"vz:0"}`), ""},
		{viz1, []byte(`{"format":"foo","qri":"vz:0","visualizations":{"colors":{"background":"#000000","bars":"#ffffff"},"type":"bar"}}`), ""},
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
	vc := Viz{Qri: KindViz.String(), Format: "foo"}
	cases := []struct {
		value interface{}
		out   *Viz
		err   string
	}{
		{vc, &vc, ""},
		{&vc, &vc, ""},
		{[]byte("{\"qri\":\"vz:0\"}"), &Viz{Qri: KindViz.String()}, ""},
		{5, nil, "couldn't parse Viz, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalViz(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if diff := compareVizs(c.out, got); diff != "" {
			t.Errorf("case %d Viz mismatch (-want +got):\n%s", i, diff)
			continue
		}
	}
}
