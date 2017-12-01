package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-datastore"

	"testing"
)

func TestTransformAssign(t *testing.T) {
	expect := &Transform{
		path:       datastore.NewKey("path"),
		Syntax:     "a",
		AppVersion: "b",
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
		AppVersion: "change",
		Config: map[string]interface{}{
			"foo": "baz",
		},
		// Abstract:  nil,
		Resources: nil,
	}

	got.Assign(&Transform{
		Syntax:     "a",
		AppVersion: "b",
		Config: map[string]interface{}{
			"foo": "bar",
		},
		// Abstract:  nil,
		Resources: nil,
	}, &Transform{
		// Abstract: &AbstractTransform{
		// 	Syntax: "structure_syntax",
		// },
		Resources: map[string]*Dataset{
			"a": NewDatasetRef(datastore.NewKey("/path/to/a")),
		},
	})

	if err := CompareTransform(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareTransform(expect, got); err != nil {
		t.Error(err)
	}

	emptyMsg := &Transform{}
	emptyMsg.Assign(expect)
	if err := CompareTransform(expect, emptyMsg); err != nil {
		t.Error(err)
	}
}

func CompareTransform(a, b *Transform) error {
	if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("nil mismatch: %v != %v", a, b)
	}
	if a == nil && b == nil {
		return nil
	}
	if err := CompareStructures(a.Structure, b.Structure); err != nil {
		return err
	}
	if len(a.Resources) != len(b.Resources) {
		return fmt.Errorf("resource count mistmatch: %d != %d", len(a.Resources), len(b.Resources))
	}
	for key, val := range a.Resources {
		if err := CompareDatasets(val, b.Resources[key]); err != nil {
			return err
		}
	}
	return nil
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

		if err := CompareTransform(c.transform, got); err != nil {
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
		{&Transform{}, `{}`, nil},
		// {&Transform{Syntax: "sql", Statement: "select a from b"}, `{"outputStructure":null,"statement":"select a from b","structures":null,"syntax":"sql"}`, nil},
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
