package dataset

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestTransformDropTransientValues(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestTransformScript(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestTransformOpenScriptFile(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestTransformAssign(t *testing.T) {
	expect := &Transform{
		Path:          "path",
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
		Path: "path",
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

	if strq.Path != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strq.Path)
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

	strbytes, err := json.Marshal(&Transform{Path: "/path/to/transform"})
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
		{&Transform{Qri: KindTransform.String()}, true},
		{&Transform{Path: "foo"}, true},
		{&Transform{}, true},
		{&Transform{Syntax: "foo"}, false},
		{&Transform{SyntaxVersion: "0"}, false},
		{&Transform{ScriptPath: "foo"}, false},
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
