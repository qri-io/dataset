package dataset

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ipfs/go-datastore"
)

func TestTransformAssign(t *testing.T) {
	expect := &Transform{
		path:       datastore.NewKey("path"),
		Syntax:     "a",
		AppVersion: "change",
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
		Syntax:     "no",
		AppVersion: "b",
		Config: map[string]interface{}{
			"foo": "baz",
		},
		// Abstract:  nil,
		Resources: nil,
	}

	got.Assign(&Transform{
		Syntax:     "a",
		AppVersion: "change",
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

func TestTransformMarshalJSON(t *testing.T) {
	cases := []struct {
		q   *Transform
		out string
		err error
	}{
		{&Transform{}, `{"qri":"tf:0"}`, nil},
		{&Transform{Syntax: "sql", Data: "select a from b"}, `{"data":"select a from b","qri":"tf:0","syntax":"sql"}`, nil},
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
