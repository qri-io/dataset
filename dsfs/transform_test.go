package dsfs

import (
	"encoding/json"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

func TestLoadTransform(t *testing.T) {
	// TODO - restore
	// store := memfs.NewMapstore()
	// q := &dataset.AbstractTransform{Statement: "select * from whatever booooooo go home"}
	// a, err := SaveAbstractTransform(store, q, true)
	// if err != nil {
	// 	t.Errorf(err.Error())
	// 	return
	// }

	// if _, err = LoadTransform(store, a); err != nil {
	// 	t.Errorf(err.Error())
	// }
	// TODO - other tests & stuff
}

func TestTransformLoadAbstract(t *testing.T) {
	// store := datastore.NewMapDatastore()
	// TODO - finish dis test
}

func TestTransformSave(t *testing.T) {
	dsa := dataset.NewDatasetRef(datastore.NewKey("/path/to/dataset/a"))
	dsa.Assign(&dataset.Dataset{Title: "now dataset isn't empty "})

	store := memfs.NewMapstore()
	q := &dataset.Transform{
		Syntax:    "sweet syntax",
		Structure: &dataset.Structure{Format: dataset.CSVDataFormat, Schema: &dataset.Schema{Fields: []*dataset.Field{{Name: "its_a_field"}}}},
		// Abstract: &dataset.AbstractTransform{
		// 	Syntax:    "sweet syntax",
		// 	Statement: "select * from a",
		// 	Structure: &dataset.Structure{Format: dataset.CSVDataFormat, Schema: &dataset.Schema{Fields: []*dataset.Field{{Name: "its_a_field"}}}},
		// 	Structures: map[string]*dataset.Structure{
		// 		"a": {Format: dataset.CSVDataFormat, Schema: &dataset.Schema{Fields: []*dataset.Field{{Name: "its_a_field"}}}},
		// 	},
		// },
		Resources: map[string]*dataset.Dataset{
			"a": dsa,
		},
	}

	key, err := SaveTransform(store, q, true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	hash := "/map/QmaNayr7fAA9DyQ8q9nbfjECiYWGLNpdk2yk77uvxq6LLJ"
	if hash != key.String() {
		t.Errorf("key mismatch: %s != %s", hash, key.String())
		return
	}

	expectedEntries := 2
	if len(store.(memfs.MapStore)) != expectedEntries {
		t.Errorf("invalid number of entries added to store: %d != %d", expectedEntries, len(store.(memfs.MapStore)))
		return
	}

	f, err := store.Get(datastore.NewKey(hash))
	if err != nil {
		t.Errorf("error getting dataset file: %s", err.Error())
		return
	}

	res := &dataset.Transform{}
	if err := json.NewDecoder(f).Decode(res); err != nil {
		t.Errorf("error decoding transform json: %s", err.Error())
		return
	}

	// if !res.Abstract.IsEmpty() {
	// 	t.Errorf("expected stored transform.Abstract to be a reference")
	// }
	if !res.Structure.IsEmpty() {
		t.Errorf("expected stored transform.Structure to be a reference")
	}
	for name, ref := range res.Resources {
		if !ref.IsEmpty() {
			t.Errorf("expected stored transform reference '%s' to be empty", name)
		}
	}
}
