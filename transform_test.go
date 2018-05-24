package dataset

import (
	"bytes"
	"encoding/json"
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
		// Abstract: &AbstractTransform{
		// 	Syntax: "structure_syntax",
		// },
		Resources: map[string]*Dataset{
			"a": NewDatasetRef(datastore.NewKey("/path/to/a")),
		},
	}
	got := &Transform{
		Syntax:        "no",
		SyntaxVersion: "b",
		Config: map[string]interface{}{
			"foo": "baz",
		},
		// Abstract:  nil,
		Resources: nil,
	}

	got.Assign(&Transform{
		Syntax:        "a",
		SyntaxVersion: "change",
		Config: map[string]interface{}{
			"foo": "bar",
		},
		// Abstract:  nil,
		Resources: nil,
	}, &Transform{
		// Abstract: &AbstractTransform{
		// 	Syntax: "structure_syntax",
		// },
		path: datastore.NewKey("path"),
		Resources: map[string]*Dataset{
			"a": NewDatasetRef(datastore.NewKey("/path/to/a")),
		},
	})

	if err := CompareTransforms(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareTransforms(expect, got); err != nil {
		t.Error(err)
	}

	emptyMsg := &Transform{}
	emptyMsg.Assign(expect)
	if err := CompareTransforms(expect, emptyMsg); err != nil {
		t.Error(err)
	}
}

func TestTransformUnmarshalJSON(t *testing.T) {
	cases := []struct {
		str       string
		transform *Transform
		err       error
	}{
		{`{}`, &Transform{}, nil},
		{`{ "structure" : "/path/to/structure" }`, &Transform{Structure: &Structure{path: datastore.NewKey("/path/to/structure")}}, nil},
		// {`{ "syntax" : "ql", "statement" : "select a from b" }`, &Transform{Syntax: "ql", Statement: "select a from b"}, nil},
	}

	for i, c := range cases {
		got := &Transform{}
		if err := json.Unmarshal([]byte(c.str), got); err != c.err {
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
		{&Transform{Syntax: "sql", ScriptPath: "foo.sky"}, `{"qri":"tf:0","scriptPath":"foo.sky","syntax":"sql"}`, nil},
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
		{&Transform{Syntax: "sql", ScriptPath: "foo.sky"}, `{"qri":"tf:0","scriptPath":"foo.sky","syntax":"sql"}`, nil},
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
		{&Transform{Resources: map[string]*Dataset{}}, false},
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
		{Resources: map[string]*Dataset{"foo": &Dataset{DataPath: "foo"}}},
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
		{&TransformPod{Resources: []byte("foo")}, "decoding transform resources: invalid character 'o' in literal false (expecting 'a')"},
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
