package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func compareReadmes(a, b *Readme) string {
	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(Readme{}))
}

func TestReadmeDropTransientValues(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestReadmeDropDerivedValues(t *testing.T) {
	rm := &Readme{
		Path: "/ipfs/QmHash",
		Qri:  "oh you know it's qri",
	}

	rm.DropDerivedValues()

	if diff := compareReadmes(rm, &Readme{}); diff != "" {
		t.Errorf("expected dropping a readme of only derived values to be empty. diff (-want +got):\n%s", diff)
	}
}

func TestReadmeScript(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestReadmeOpenScriptFile(t *testing.T) {
	t.Log("TODO (b5)")
}

var readme1 = &Readme{
	Format:       "foo",
	Qri:          KindReadme.String(),
	ScriptPath:   "one",
	RenderedPath: "one",
}

var readme2 = &Readme{
	Format:     "bar",
	Qri:        KindReadme.String(),
	ScriptPath: "two",
}

var readme3 = &Readme{
	Format:     "bar",
	Qri:        KindReadme.String(),
	ScriptPath: "three",
}

func TestReadmeAssign(t *testing.T) {
	cases := []struct {
		got    *Readme
		assign *Readme
		expect *Readme
	}{
		{nil, nil, nil},
		{&Readme{}, readme1, readme1},
		{&Readme{
			Format:     "bar",
			Qri:        KindReadme.String(),
			ScriptPath: "replace me",
		},
			readme2, readme2},
		{&Readme{
			Path:       "foo",
			Format:     "foo",
			Qri:        KindReadme.String(),
			ScriptPath: "bat",
		},
			&Readme{Path: "bar", Format: "bar", RenderedPath: "rendered"},
			&Readme{
				Path:         "bar",
				Format:       "bar",
				Qri:          KindReadme.String(),
				ScriptPath:   "bat",
				RenderedPath: "rendered",
			}},
	}
	for i, c := range cases {
		c.got.Assign(c.assign)
		if diff := compareReadmes(c.expect, c.got); diff != "" {
			t.Errorf("case %d result mismatch. (-want +got):\n%s", i, diff)
		}
	}

}

func TestReadmeIsEmpty(t *testing.T) {
	cases := []struct {
		vc       *Readme
		expected bool
	}{
		{&Readme{Qri: KindReadme.String()}, true},
		{&Readme{ScriptPath: "foo"}, false},
		{&Readme{RenderedPath: "foo"}, false},
		{&Readme{}, true},
		{&Readme{Path: "foo"}, true},
	}

	for i, c := range cases {
		if c.vc.IsEmpty() != c.expected {
			t.Errorf("case %d improperly reported visconfig as empty == %v", i, c.expected)
			continue
		}
	}
}

func TestReadmeShallowCompare(t *testing.T) {
	cases := []struct {
		a, b   *Readme
		expect bool
	}{
		{nil, nil, true},
		{nil, &Readme{}, false},
		{&Readme{}, nil, false},

		{&Readme{Path: "a"}, &Readme{Path: "NOT_A"}, true},

		{&Readme{Qri: "a"}, &Readme{Qri: "b"}, false},
		{&Readme{Format: "a"}, &Readme{Format: "b"}, false},
		{&Readme{ScriptBytes: []byte("a")}, &Readme{ScriptBytes: []byte("b")}, false},
		{&Readme{ScriptPath: "a"}, &Readme{ScriptPath: "b"}, false},
		{&Readme{RenderedPath: "a"}, &Readme{RenderedPath: "b"}, false},

		{
			&Readme{Qri: "a", Format: "a", ScriptBytes: []byte("a"), ScriptPath: "a", RenderedPath: "a"},
			&Readme{Qri: "a", Format: "a", ScriptBytes: []byte("a"), ScriptPath: "a", RenderedPath: "a"},
			true,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			got := c.a.ShallowCompare(c.b)
			if c.expect != got {
				t.Errorf("wanted %t, got %t", c.expect, got)
			}
		})
	}
}

func TestReadmeUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Readme
		err      string
	}{
		{"testdata/readmes/invalidJSON.json", nil, `invalid character 'I' looking for beginning of value`},
		{"testdata/readmes/readmeconfig1.json", readme1, ""},
		{"testdata/readmes/readmeconfig2.json", readme2, ""},
		{"testdata/readmes/readmeconfig3.json", readme3, ""},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		vc := &Readme{}
		err = json.Unmarshal(data, vc)
		if err != nil {
			if err.Error() != c.err {
				t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
				continue
			} else {
				continue
			}
		}

		if diff := compareReadmes(vc, c.result); diff != "" {
			t.Errorf("case %d resource comparison error (-want +got):\n%s", i, diff)
			continue
		}
	}

	vc := &Readme{}
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

func TestReadmeMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Readme
		out []byte
		err string
	}{
		{&Readme{}, []byte(`{"qri":"rm:0"}`), ""},
		{&Readme{Qri: KindReadme.String()}, []byte(`{"qri":"rm:0"}`), ""},
		{&Readme{Format: "foo", Qri: KindReadme.String()}, []byte(`{"format":"foo","qri":"rm:0"}`), ""},
		{readme1, []byte(`{"format":"foo","qri":"rm:0","renderedPath":"one","scriptPath":"one"}`), ""},
		{&Readme{Path: "/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U"}, []byte(`"/map/QmXo5LE3WVfKZKzTrrgtUUX3nMK4VREKTAoBu5WAGECz4U"`), ""},
		{&Readme{Path: "/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"}, []byte(`"/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"`), ""},
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

	vcbytes, err := json.Marshal(&Readme{Path: "/path/to/Readme"})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(vcbytes, []byte("\"/path/to/Readme\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(vcbytes), "\"/path/to/Readme\"")
	}
}

func TestReadmeMarshalJSONObject(t *testing.T) {
	cases := []struct {
		in  *Readme
		out []byte
		err string
	}{
		{&Readme{}, []byte(`{"qri":"rm:0"}`), ""},
		{&Readme{Qri: KindReadme.String()}, []byte(`{"qri":"rm:0"}`), ""},
		{&Readme{Format: "foo", Qri: KindReadme.String()}, []byte(`{"format":"foo","qri":"rm:0"}`), ""},
		{readme1, []byte(`{"format":"foo","qri":"rm:0","visualizations":{"colors":{"background":"#000000","bars":"#ffffff"},"type":"bar"}}`), ""},
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

func TestUnmarshalReadme(t *testing.T) {
	vc := Readme{Qri: KindReadme.String(), Format: "foo"}
	cases := []struct {
		value interface{}
		out   *Readme
		err   string
	}{
		{vc, &vc, ""},
		{&vc, &vc, ""},
		{[]byte("{\"qri\":\"rm:0\"}"), &Readme{Qri: KindReadme.String()}, ""},
		{5, nil, "couldn't parse Readme, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalReadme(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if diff := compareReadmes(c.out, got); diff != "" {
			t.Errorf("case %d Readme mismatch (-want +got):\n%s", i, diff)
			continue
		}
	}
}
