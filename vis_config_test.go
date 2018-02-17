package dataset

import (
	"bytes"
	"encoding/json"
	"github.com/ipfs/go-datastore"
	"io/ioutil"
	"testing"
)

var VisConfig1 = &VisConfig{
	Format: "foo",
	Qri:    KindVisConfig,
	Visualizations: map[string]interface{}{
		"type": "bar",
		"colors": map[string]interface{}{
			"bars":       "#ffffff",
			"background": "#000000",
		},
	},
}

var VisConfig2 = &VisConfig{
	Format:         "bar",
	Qri:            KindVisConfig,
	Visualizations: []interface{}{"foo", "bar"},
}

var VisConfig3 = &VisConfig{
	Format:         "bar",
	Qri:            KindVisConfig,
	Visualizations: float64(10),
}

func TestVisConfigSetPath(t *testing.T) {
	cases := []struct {
		path   string
		expect *VisConfig
	}{
		{"", &VisConfig{path: datastore.Key{}}},
		{"path", &VisConfig{path: datastore.NewKey("path")}},
	}

	for i, c := range cases {
		got := &VisConfig{}
		got.SetPath(c.path)
		if err := CompareVisConfigs(c.expect, got); err != nil {
			t.Errorf("case %d error: %s", i, err)
			continue
		}
	}
}

func TestVisConfigAssign(t *testing.T) {
	cases := []struct {
		got    *VisConfig
		assign *VisConfig
		expect *VisConfig
		err    string
	}{
		{nil, nil, nil, ""},
		{&VisConfig{}, VisConfig1, VisConfig1, ""},
		{&VisConfig{
			Format:         "bar",
			Qri:            KindVisConfig,
			Visualizations: float64(10),
		},
			VisConfig2, VisConfig2, ""},
		{&VisConfig{
			Format:         "bar",
			Qri:            KindVisConfig,
			Visualizations: float64(10),
		},
			VisConfig2, VisConfig3, "Visualizations not equal"},
		{&VisConfig{
			path:           datastore.NewKey("foo"),
			Format:         "foo",
			Qri:            KindVisConfig,
			Visualizations: float64(10),
		},
			&VisConfig{path: datastore.NewKey("bar"), Format: "bar"},
			&VisConfig{
				path:           datastore.NewKey("foo"),
				Format:         "bar",
				Qri:            KindVisConfig,
				Visualizations: float64(10),
			}, ""},
	}
	for i, c := range cases {
		c.got.Assign(c.assign)
		err := CompareVisConfigs(c.expect, c.got)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
		}
	}

}

func TestVisConfigIsEmpty(t *testing.T) {
	cases := []struct {
		vc       *VisConfig
		expected bool
	}{
		{&VisConfig{Qri: KindVisConfig}, true},
		// {&VisConfig{DataPath: "foo"}, false},
		{&VisConfig{Visualizations: "bar"}, false},
		{&VisConfig{}, true},
		{&VisConfig{path: datastore.NewKey("foo")}, true},
	}

	for i, c := range cases {
		if c.vc.IsEmpty() != c.expected {
			t.Errorf("case %d improperly reported visconfig as empty == %v", i, c.expected)
			continue
		}
	}
}

func TestVisConfigUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *VisConfig
		err      string
	}{
		{"testdata/visconfigs/invalidJSON.json", nil, `invalid character 'I' looking for beginning of value`},
		{"testdata/visconfigs/visconfig1.json", VisConfig1, ""},
		{"testdata/visconfigs/visconfig2.json", VisConfig2, ""},
		{"testdata/visconfigs/visconfig3.json", VisConfig3, ""},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		vc := &VisConfig{}
		err = json.Unmarshal(data, vc)
		if err != nil {
			if err.Error() != c.err {
				t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
				continue
			} else {
				continue
			}
		}

		if err = CompareVisConfigs(vc, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}
	}

	vc := &VisConfig{}
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

func TestVisConfigMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *VisConfig
		out []byte
		err string
	}{
		{&VisConfig{}, []byte(`{"qri":"vc:0"}`), ""},
		{&VisConfig{Qri: KindVisConfig}, []byte(`{"qri":"vc:0"}`), ""},
		{&VisConfig{Format: "foo", Qri: KindVisConfig}, []byte(`{"format":"foo","qri":"vc:0"}`), ""},
		{VisConfig1, []byte(`{"format":"foo","qri":"vc:0","visualizations":{"colors":{"background":"#000000","bars":"#ffffff"},"type":"bar"}}`), ""},
		{&VisConfig{path: datastore.NewKey("/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U")}, []byte(`"/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U"`), ""},
		{&VisConfig{path: datastore.NewKey("/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD")}, []byte(`"/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"`), ""},
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

	vcbytes, err := json.Marshal(&VisConfig{path: datastore.NewKey("/path/to/VisConfig")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(vcbytes, []byte("\"/path/to/VisConfig\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(vcbytes), "\"/path/to/VisConfig\"")
	}
}

// func TestVisConfigMarshalJSONObject(t *testing.T) {
// 	cases := []struct {
// 		in  *VisConfig
// 		out []byte
// 		err string
// 	}{
// 		{&VisConfig{}, []byte(`{"qri":"vc:0"}`), ""},
// 		{&VisConfig{Qri: KindVisConfig}, []byte(`{"qri":"vc:0"}`), ""},
// 		{&VisConfig{Format: "foo", Qri: KindVisConfig}, []byte(`{"format":"foo","qri":"vc:0"}`), ""},
// 		{VisConfig1, []byte(`{"format":"foo","qri":"vc:0","visualizations":{"colors":{"background":"#000000","bars":"#ffffff"},"type":"bar"}}`), ""},
// 		{&VisConfig{path: datastore.NewKey("/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U")}, []byte(`{"qri":"vc:0"}`), ""},
// 	}

// 	for i, c := range cases {
// 		got, err := c.in.MarshalJSONObject()
// 		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
// 			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
// 			continue
// 		}

// 		if string(c.out) != string(got) {
// 			t.Errorf("case %d, %s != %s", i, string(c.out), string(got))
// 			continue
// 		}
// 	}

// 	vcbytes, err := json.Marshal(&VisConfig{path: datastore.NewKey("/path/to/VisConfig")})
// 	if err != nil {
// 		t.Errorf("unexpected string marshal error: %s", err.Error())
// 		return
// 	}

// 	if !bytes.Equal(vcbytes, []byte("\"/path/to/VisConfig\"")) {
// 		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(vcbytes), "\"/path/to/VisConfig\"")
// 	}
// }

func TestVisConfigMarshalJSONObject(t *testing.T) {
	cases := []struct {
		in  *VisConfig
		out []byte
		err string
	}{
		{&VisConfig{}, []byte(`{"qri":"vc:0"}`), ""},
		{&VisConfig{Qri: KindVisConfig}, []byte(`{"qri":"vc:0"}`), ""},
		{&VisConfig{Format: "foo", Qri: KindVisConfig}, []byte(`{"format":"foo","qri":"vc:0"}`), ""},
		{VisConfig1, []byte(`{"format":"foo","qri":"vc:0","visualizations":{"colors":{"background":"#000000","bars":"#ffffff"},"type":"bar"}}`), ""},
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

func TestUnmarshalVisConfig(t *testing.T) {
	vc := VisConfig{Qri: KindVisConfig, Format: "foo"}
	cases := []struct {
		value interface{}
		out   *VisConfig
		err   string
	}{
		{vc, &vc, ""},
		{&vc, &vc, ""},
		{[]byte("{\"qri\":\"vc:0\"}"), &VisConfig{Qri: KindVisConfig}, ""},
		{5, nil, "couldn't parse VisConfig, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalVisConfig(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareVisConfigs(c.out, got); err != nil {
			t.Errorf("case %d VisConfig mismatch: %s", i, err.Error())
			continue
		}
	}
}
