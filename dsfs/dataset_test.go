package dsfs

import (
	"encoding/json"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

func TestLoadDataset(t *testing.T) {
	store := memfs.NewMapstore()
	apath, err := SaveDataset(store, AirportCodes, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	_, err = LoadDataset(store, apath)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestDatasetSave(t *testing.T) {
	store := memfs.NewMapstore()
	resource := dataset.NewDatasetRef(datastore.NewKey("resource1"))
	resource.Title = "now resource.Empty() == false"

	datapath, err := store.Put(memfs.NewMemfileBytes("data.csv", []byte("hello world")), false)
	if err != nil {
		t.Errorf("error putting test data in store: %s", err.Error())
		return
	}

	ds := &dataset.Dataset{
		Title: "test store",
		Structure: &dataset.Structure{
			Format: dataset.CSVDataFormat,
			Schema: &dataset.Schema{
				Fields: []*dataset.Field{},
			},
		},
		AbstractStructure: &dataset.Structure{
			Format: dataset.CSVDataFormat,
			Schema: &dataset.Schema{
				Fields: []*dataset.Field{},
			},
		},
		Transform: &dataset.Transform{
			Syntax: "dunno",
			Abstract: &dataset.AbstractTransform{
				Statement: "test statement",
			},
			Resources: map[string]*dataset.Dataset{
				"test": resource,
			},
		},
		AbstractTransform: &dataset.AbstractTransform{Statement: "select fooo from bar"},
		Data:              datapath,
	}

	key, err := SaveDataset(store, ds, true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	hash := "/map/QmPGKFvhSKWNcn35D69eKhjUms7Y25MQR6TtxhgiChSkQT"
	if hash != key.String() {
		t.Errorf("key mismatch: %s != %s", hash, key.String())
		return
	}

	expectedEntries := 5
	if len(store.(memfs.MapStore)) != expectedEntries {
		t.Error("invalid number of entries added to store: %d != %d", expectedEntries, len(store.(memfs.MapStore)))
		return
	}

	f, err := store.Get(datastore.NewKey(hash))
	if err != nil {
		t.Errorf("error getting dataset file: %s", err.Error())
		return
	}

	result := &dataset.Dataset{}
	if err := json.NewDecoder(f).Decode(result); err != nil {
		t.Errorf("error decoding dataset json: %s", err.Error())
		return
	}

	if !result.AbstractTransform.IsEmpty() {
		t.Errorf("expected stored dataset.AbstractTransform to be a reference")
	}
	if !result.Transform.IsEmpty() {
		t.Errorf("expected stored dataset.Transform to be a reference")
	}
	if !result.Structure.IsEmpty() {
		t.Errorf("expected stored dataset.Structure to be a reference")
	}
	if !result.AbstractStructure.IsEmpty() {
		t.Errorf("expected stored dataset.AbstractStructure to be a reference")
	}

	qf, err := store.Get(result.Transform.Path())
	if err != nil {
		t.Errorf("error getting transform file: %s", err.Error())
		return
	}

	q := &dataset.Transform{}
	if err := json.NewDecoder(qf).Decode(q); err != nil {
		t.Errorf("error decoding transform json: %s", err.Error())
		return
	}

	if !q.Abstract.IsEmpty() {
		t.Errorf("expected stored transform.Abstract to be a reference")
	}
	for name, ref := range q.Resources {
		if !ref.IsEmpty() {
			t.Errorf("expected stored transform reference '%s' to be empty", name)
		}
	}
}
