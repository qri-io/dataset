package dsfs

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	crypto "github.com/libp2p/go-libp2p-crypto"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
)

func TestLoadTransform(t *testing.T) {
	// TODO - restore
	// store := cafs.NewMapstore()
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

func TestSaveTransform(t *testing.T) {
	dsa := dataset.NewDatasetRef("/path/to/dataset/a")
	dsa.Assign(&dataset.Dataset{Meta: &dataset.Meta{Title: "now dataset isn't empty"}})

	store := cafs.NewMapstore()
	q := &dataset.Transform{
		Syntax: "sweet syntax",
		Resources: map[string]*dataset.TransformResource{
			"a": &dataset.TransformResource{Path: dsa.Path},
		},
	}

	key, err := SaveTransform(store, q, true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	hash := "/map/QmVyDCTY92ouEFBvfwQzgsc4n2owpKJmnPeWHVV4s6FuXD"
	if hash != key {
		t.Errorf("key mismatch: %s != %s", hash, key)
		return
	}

	expectedEntries := 1
	if len(store.Files) != expectedEntries {
		t.Errorf("invalid number of entries added to store: %d != %d", expectedEntries, len(store.Files))
		return
	}

	f, err := store.Get(hash)
	if err != nil {
		t.Errorf("error getting dataset file: %s", err.Error())
		return
	}

	res := &dataset.Transform{}
	if err := json.NewDecoder(f).Decode(res); err != nil {
		t.Errorf("error decoding transform json: %s", err.Error())
		return
	}

	for name, ref := range res.Resources {
		if ref.Path == "" {
			t.Errorf("expected stored transform reference '%s' to have a path", name)
		}
	}
}

func TestLoadTransformScript(t *testing.T) {
	store := cafs.NewMapstore()
	privKey, err := crypto.UnmarshalPrivateKey(testPk)
	if err != nil {
		t.Fatalf("error unmarshaling private key: %s", err.Error())
	}

	_, err = LoadTransformScript(store, "")
	if err == nil {
		t.Error("expected load empty key to fail")
	}

	tc, err := dstest.NewTestCaseFromDir("testdata/cities_no_commit_title")
	if err != nil {
		t.Fatal(err.Error())
	}
	path, err := CreateDataset(store, tc.Input, nil, tc.BodyFile(), nil, privKey, true)
	if err != nil {
		t.Fatal(err.Error())
	}

	if _, err = LoadTransformScript(store, path); err != ErrNoTransform {
		t.Errorf("expected no transform script error. got: %s", err)
	}

	tc, err = dstest.NewTestCaseFromDir("testdata/all_fields")
	if err != nil {
		t.Fatal(err.Error())
	}
	tsf, _ := tc.TransformScriptFile()
	transformPath, err := store.Put(tsf, true)
	if err != nil {
		t.Fatal(err.Error())
	}
	tc.Input.Transform.ScriptPath = transformPath
	path, err = CreateDataset(store, tc.Input, nil, tc.BodyFile(), nil, privKey, true)
	if err != nil {
		t.Fatal(err.Error())
	}

	file, err := LoadTransformScript(store, path)
	if err != nil {
		t.Fatalf("expected transform script to load. got: %s", err)
	}

	tsf, _ = tc.TransformScriptFile()

	r := &EqualReader{file, tsf}
	if _, err := ioutil.ReadAll(r); err != nil {
		t.Error(err.Error())
	}
}
