package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/ipfs/go-datastore"
)

func TestTransformSetPath(t *testing.T) {
	cases := []struct {
		path   string
		expect *Transform
	}{
		{"", &Transform{path: datastore.Key{}}},
		{"path", &Transform{path: datastore.NewKey("path")}},
	}

	for i, c := range cases {
		got := &Transform{}
		got.SetPath(c.path)
		if err := CompareTransforms(c.expect, got); err != nil {
			t.Errorf("case %d error: %s", i, err)
			continue
		}
	}
}

func TestTransformAssign(t *testing.T) {
	expect := &Transform{
		path:          datastore.NewKey("path"),
		Syntax:        "a",
		SyntaxVersion: "change",
		Config: map[string]interface{}{
			"foo": "bar",
		},
		Resources: map[string]*TransformResource{
			"a": &TransformResource{Path: "/path/to/a"},
		},
	}
	got := &Transform{
		Syntax:        "no",
		SyntaxVersion: "b",
		Config: map[string]interface{}{
			"foo": "baz",
		},
		Resources: nil,
	}

	got.Assign(&Transform{
		Syntax:        "a",
		SyntaxVersion: "change",
		Config: map[string]interface{}{
			"foo": "bar",
		},
		Resources: nil,
	}, &Transform{
		path: datastore.NewKey("path"),
		Resources: map[string]*TransformResource{
			"a": &TransformResource{Path: "/path/to/a"},
		},
	})

	if err := CompareTransforms(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareTransforms(expect, got); err != nil {
		t.Error(err)
	}

	emptyTf := &Transform{}
	emptyTf.Assign(expect)
	if err := CompareTransforms(expect, emptyTf); err != nil {
		t.Error(err)
	}
}

func TestTransformUnmarshalJSON(t *testing.T) {
	cases := []struct {
		str       string
		transform *Transform
		err       string
	}{
		{`{}`, &Transform{}, ""},
		{`{ "structure" : "/path/to/structure" }`, &Transform{Structure: &Structure{path: datastore.NewKey("/path/to/structure")}}, ""},
		{`{"resources":{"foo": "/not/a/real/path"}}`, &Transform{Resources: map[string]*TransformResource{"foo": &TransformResource{Path: "/not/a/real/path"}}}, ""},
		{`{"resources":{"foo": { "path":     "/not/a/real/path"`, &Transform{}, "unexpected end of JSON input"},
		{`{"resources":{"foo": { "path":"/not/a/real/path"}}}`, &Transform{Resources: map[string]*TransformResource{"foo": &TransformResource{Path: "/not/a/real/path"}}}, ""},
	}

	for i, c := range cases {
		got := &Transform{}
		err := json.Unmarshal([]byte(c.str), got)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if err := CompareTransforms(c.transform, got); err != nil {
			t.Errorf("case %d transform mismatch: %s", i, err)
			continue
		}
	}

	strq := &Transform{}
	path := "/path/to/transform"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strq); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strq.path.String() != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strq.path)
		return
	}
}

func TestTransformMarshalJSONObject(t *testing.T) {
	cases := []struct {
		q   *Transform
		out string
		err error
	}{
		{&Transform{}, `{"qri":"tf:0"}`, nil},
		{&Transform{Syntax: "sql", ScriptPath: "foo.star"}, `{"qri":"tf:0","scriptPath":"foo.star","syntax":"sql"}`, nil},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.q)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if string(data) != c.out {
			t.Errorf("case %d result mismatch. expected: %s, got: %s", i, c.out, string(data))
			continue
		}
	}

	strbytes, err := json.Marshal(&Transform{path: datastore.NewKey("/path/to/transform")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/transform\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/transform\"")
	}
}

func TestTransformMarshalJSON(t *testing.T) {
	cases := []struct {
		q   *Transform
		out string
		err error
	}{
		{&Transform{}, `{"qri":"tf:0"}`, nil},
		{&Transform{Syntax: "sql", ScriptPath: "foo.star"}, `{"qri":"tf:0","scriptPath":"foo.star","syntax":"sql"}`, nil},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.q)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		check := &map[string]interface{}{}
		err = json.Unmarshal(data, check)
		if err != nil {
			t.Errorf("case %d error: failed to unmarshal to object: %s", i, err.Error())
			continue
		}
	}

}

func TestTransformIsEmpty(t *testing.T) {
	cases := []struct {
		tf       *Transform
		expected bool
	}{
		{&Transform{Qri: KindTransform}, true},
		{&Transform{path: datastore.NewKey("foo")}, true},
		{&Transform{}, true},
		{&Transform{Syntax: "foo"}, false},
		{&Transform{SyntaxVersion: "0"}, false},
		{&Transform{ScriptPath: "foo"}, false},
		{&Transform{Structure: nil}, true},
		{&Transform{Structure: &Structure{}}, false},
		{&Transform{Config: nil}, true},
		{&Transform{Config: map[string]interface{}{}}, false},
		{&Transform{Resources: nil}, true},
		{&Transform{Resources: map[string]*TransformResource{}}, false},
	}

	for i, c := range cases {
		if c.tf.IsEmpty() != c.expected {
			t.Errorf("case %d improperly reported transform as empty == %v", i, c.expected)
			continue
		}
	}
}

func TestTransformCoding(t *testing.T) {
	cases := []*Transform{
		{},
		{SyntaxVersion: "foo"},
		{Config: map[string]interface{}{"foo": "foo"}},
		{ScriptPath: "foo"},
		{path: datastore.NewKey("/foo")},
		{Qri: KindTransform},
		{Resources: map[string]*TransformResource{"foo": &TransformResource{Path: "foo"}}},
		{Syntax: "foo"},
		{Structure: &Structure{Format: CBORDataFormat}},
	}

	for i, c := range cases {
		cd := c.Encode()
		got := &Transform{}
		if err := got.Decode(cd); err != nil {
			t.Errorf("case %d unexpected error '%s'", i, err)
			continue
		}

		if err := CompareTransforms(c, got); err != nil {
			t.Errorf("case %d dataset mismatch: %s", i, err.Error())
			continue
		}
	}
}

func TestTransformDecode(t *testing.T) {
	cases := []struct {
		ct  *TransformPod
		err string
	}{
		{&TransformPod{}, ""},
		{&TransformPod{Resources: map[string]interface{}{"foo": 0}}, "resource 'foo': json: cannot unmarshal number into Go value of type dataset.transformResource"},
		{&TransformPod{Structure: &StructurePod{Format: "foo"}}, "invalid data format: `foo`"},
	}

	for i, c := range cases {
		got := &Transform{}
		err := got.Decode(c.ct)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestTransformPodAssign(t *testing.T) {
	expect := &TransformPod{
		Path:          "path",
		Syntax:        "a",
		SyntaxVersion: "change",
		Config: map[string]interface{}{
			"foo": "bar",
		},
		Resources: map[string]interface{}{"a": "b"},
	}
	got := &TransformPod{
		Syntax:        "no",
		SyntaxVersion: "b",
		Config: map[string]interface{}{
			"foo": "baz",
		},
		Resources: nil,
	}

	got.Assign(&TransformPod{
		Syntax:        "a",
		SyntaxVersion: "change",
		Config: map[string]interface{}{
			"foo": "bar",
		},
		Resources: nil,
	}, &TransformPod{
		Path:      "path",
		Resources: map[string]interface{}{"a": "b"},
	})

	if err := CompareTransformPods(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareTransformPods(expect, got); err != nil {
		t.Error(err)
	}

	emptyTf := &TransformPod{}
	emptyTf.Assign(expect)
	if err := CompareTransformPods(expect, emptyTf); err != nil {
		t.Error(err)
	}
}

func CompareTransformPods(a, b *TransformPod) error {
	if !reflect.DeepEqual(a.Config, b.Config) {
		return fmt.Errorf("Config: %s != %s", a.Config, b.Config)
	}
	if a.TransformPath != b.TransformPath {
		return fmt.Errorf("TransformPath: %s != %s", a.TransformPath, b.TransformPath)
	}
	if a.Path != b.Path {
		return fmt.Errorf("Path: %s != %s", a.Path, b.Path)
	}
	if a.Qri != b.Qri {
		return fmt.Errorf("Qri: %s != %s", a.Qri, b.Qri)
	}
	if !reflect.DeepEqual(a.Resources, b.Resources) {
		return fmt.Errorf("Resources: %v != %v", a.Resources, b.Resources)
	}
	if !reflect.DeepEqual(a.Secrets, b.Secrets) {
		return fmt.Errorf("Secrets: %v != %v", a.Secrets, b.Secrets)
	}
	if a.Structure != b.Structure {
		return fmt.Errorf("Structure: %v != %v", a.Structure, b.Structure)
	}
	if a.ScriptPath != b.ScriptPath {
		return fmt.Errorf("ScriptPath: %s != %s", a.ScriptPath, b.ScriptPath)
	}
	if !bytes.Equal(a.ScriptBytes, b.ScriptBytes) {
		return fmt.Errorf("ScriptBytes: %v != %v", a.ScriptBytes, b.ScriptBytes)
	}
	if a.Syntax != b.Syntax {
		return fmt.Errorf("Syntax: %s != %s", a.Syntax, b.Syntax)
	}
	if a.SyntaxVersion != b.SyntaxVersion {
		return fmt.Errorf("SyntaxVersion: %s != %s", a.SyntaxVersion, b.SyntaxVersion)
	}
	return nil
}
