package dsfs

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

// func TestPrepareDataset(t *testing.T) {
// 	store := memfs.NewMapstore()

// 	cases := []struct {
// 		in, out *dataset.Dataset
// 		err     string
// 	}{}

// 	for i, c := range cases {
// 		err := PrepareDataset(store, ds)
// 		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
// 			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
// 			continue
// 		}
// 	}
// }

func TestLoadDataset(t *testing.T) {
	store := memfs.NewMapstore()

	data, err := ioutil.ReadFile("testdata/complete.json")
	if err != nil {
		t.Errorf("error loading test dataset: %s", err.Error())
		return
	}
	ds := &dataset.Dataset{}
	if err := ds.UnmarshalJSON(data); err != nil {
		t.Errorf("error unmarshaling test dataset: %s", err.Error())
	}
	apath, err := SaveDataset(store, ds, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	_, err = LoadDataset(store, apath)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	cases := []struct {
		ds  *dataset.Dataset
		err string
	}{
		{dataset.NewDatasetRef(datastore.NewKey("/bad/path")),
			"error loading dataset: error getting file bytes: datastore: key not found"},
		{&dataset.Dataset{
			Title:     "bad structure",
			Structure: dataset.NewStructureRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset structure: error loading structure file: datastore: key not found"},
		{&dataset.Dataset{
			Title:     "bad structure",
			Transform: dataset.NewTransformRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset transform: error loading transform raw data: datastore: key not found"},
		{&dataset.Dataset{
			Title:  "bad structure",
			Commit: dataset.NewCommitMsgRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset commit: error loading commit file: datastore: key not found"},
	}

	for i, c := range cases {
		path := c.ds.Path()
		if !c.ds.IsEmpty() {
			dsf, err := JSONFile(PackageFileDataset.String(), c.ds)
			if err != nil {
				t.Errorf("case %d error generating json file: %s", i, err.Error())
				continue
			}
			path, err = store.Put(dsf, true)
			if err != nil {
				t.Errorf("case %d error putting file in store", i, err.Error())
				continue
			}
		}

		_, err = LoadDataset(store, path)
		if !(err != nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}

}

func TestSaveDataset(t *testing.T) {
	store := memfs.NewMapstore()

	if _, err := SaveDataset(store, nil, true); err == nil || err.Error() != "cannot save empty dataset" {
		t.Errorf("didn't reject empty dataset: %s", err)
	}
	if _, err := SaveDataset(store, &dataset.Dataset{}, true); err == nil || err.Error() != "cannot save empty dataset" {
		t.Errorf("didn't reject empty dataset: %s", err)
	}

	cases := []struct {
		infile      string
		path        datastore.Key
		repoEntries int
		err         string
	}{
		{"testdata/cities.json", datastore.NewKey("/map/QmQDJiMKBXGJTXDJm4KQ6ddzggQhYi4PPHj2F6bqJCKwvv"), 2, ""},
		{"testdata/complete.json", datastore.NewKey("/map/Qmdp2mMbLqhZCAdtHtVqA8GjRaxgdvPWyUEjVU1yCqcgyw"), 8, ""},
	}

	for i, c := range cases {
		indata, err := ioutil.ReadFile(c.infile)
		if err != nil {
			t.Errorf("case %d error opening test infile: %s", i, err.Error())
			continue
		}

		ds := &dataset.Dataset{}
		if err := ds.UnmarshalJSON(indata); err != nil {
			t.Errorf("case %d error unmarhshalling test file: %s ", i, err.Error())
			continue
		}

		got, err := SaveDataset(store, ds, true)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if !c.path.Equal(got) {
			t.Errorf("case %d path mismatch. expected: '%s', got: '%s'", i, c.path, got)
			continue
		}

		if len(store.(memfs.MapStore)) != c.repoEntries {
			t.Errorf("case %d invalid number of entries in store: %d != %d", i, c.repoEntries, len(store.(memfs.MapStore)))
			str, err := store.(memfs.MapStore).Print()
			if err != nil {
				panic(err)
			}
			t.Log(str)
			continue
		}

		f, err := store.Get(got)
		if err != nil {
			t.Errorf("error getting dataset file: %s", err.Error())
			continue
		}

		ref := &dataset.Dataset{}
		if err := json.NewDecoder(f).Decode(ref); err != nil {
			t.Errorf("error decoding dataset json: %s", err.Error())
			continue
		}

		if ref.Abstract != nil {
			if !ref.Abstract.IsEmpty() {
				t.Errorf("expected stored dataset.Abstract to be a reference")
			}
			// Abstract paths shouldnt' be loaded
			ds.Abstract = dataset.NewDatasetRef(ref.Abstract.Path())
		}

		if ref.Transform != nil {
			if !ref.Transform.IsEmpty() {
				t.Errorf("expected stored dataset.Transform to be a reference")
			}
			ds.Transform.Assign(dataset.NewTransformRef(ref.Transform.Path()))
		}
		if ref.AbstractTransform != nil {
			if !ref.AbstractTransform.IsEmpty() {
				t.Errorf("expected stored dataset.AbstractTransform to be a reference")
			}
			// Abstract transforms aren't loaded
			ds.AbstractTransform = dataset.NewTransformRef(ref.AbstractTransform.Path())
		}
		if ref.Structure != nil {
			if !ref.Structure.IsEmpty() {
				t.Errorf("expected stored dataset.Structure to be a reference")
			}
			ds.Structure.Assign(dataset.NewStructureRef(ref.Structure.Path()))
		}

		ds.Assign(dataset.NewDatasetRef(got))
		result, err := LoadDataset(store, got)
		if err != nil {
			t.Errorf("case %d unexpected error loading dataset: %s", i, err)
			continue
		}

		if err := dataset.CompareDatasets(ds, result); err != nil {
			t.Errorf("case %d comparison mismatch: %s", i, err.Error())

			d1, _ := ds.MarshalJSON()
			t.Log(string(d1))

			d, _ := result.MarshalJSON()
			t.Log(string(d))
			continue
		}
	}
}
