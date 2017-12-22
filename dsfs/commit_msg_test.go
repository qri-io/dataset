package dsfs

import (
	"github.com/qri-io/dataset"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs/memfs"
)

func TestSaveCommit(t *testing.T) {
	store := memfs.NewMapstore()
	path, err := SaveCommit(store, AirportCodesCommit, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	cmt, err := LoadCommit(store, path)
	if err != nil {
		t.Errorf("error loading saved commit message: %s", err.Error())
		return
	}

	if err := dataset.CompareCommits(AirportCodesCommit, cmt); err != nil {
		t.Errorf("saved message mismatch: %s", err.Error())
		return
	}

	// _, err = SaveCommit(store, &dataset.Dataset{}, false)
	// if err == nil {
	// 	t.Errorf("expected saving nil message to error")
	// 	return
	// }

	// expect := "error:"
	// if err.Error() != expect {
	// 	t.Errorf("save error mismatch. expected: '%s', got: '%s'", expect, err.Error())
	// }
}

func TestLoadCommit(t *testing.T) {
	store := memfs.NewMapstore()
	a, err := SaveCommit(store, AirportCodesCommit, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if _, err := LoadCommit(store, a); err != nil {
		t.Errorf(err.Error())
	}

	_, err = LoadCommit(store, datastore.NewKey("/bad/path"))
	if err == nil {
		t.Errorf("expected loading a bad path to error. got nil")
		return
	}

	expect := "error loading commit file: datastore: key not found"
	if err.Error() != expect {
		t.Errorf("error mismatch. expected: '%s', got: '%s'", expect, err.Error())
	}
}
