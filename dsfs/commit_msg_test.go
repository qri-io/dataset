package dsfs

import (
	"github.com/qri-io/dataset"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs/memfs"
)

func TestSaveCommitMsg(t *testing.T) {
	store := memfs.NewMapstore()
	path, err := SaveCommitMsg(store, AirportCodesCommitMsg, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	cmt, err := LoadCommitMsg(store, path)
	if err != nil {
		t.Errorf("error loading saved commit message: %s", err.Error())
		return
	}

	if err := dataset.CompareCommitMsgs(AirportCodesCommitMsg, cmt); err != nil {
		t.Errorf("saved message mismatch: %s", err.Error())
		return
	}

	// _, err = SaveCommitMsg(store, &dataset.Dataset{}, false)
	// if err == nil {
	// 	t.Errorf("expected saving nil message to error")
	// 	return
	// }

	// expect := "error:"
	// if err.Error() != expect {
	// 	t.Errorf("save error mismatch. expected: '%s', got: '%s'", expect, err.Error())
	// }
}

func TestLoadCommitMsg(t *testing.T) {
	store := memfs.NewMapstore()
	a, err := SaveCommitMsg(store, AirportCodesCommitMsg, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if _, err := LoadCommitMsg(store, a); err != nil {
		t.Errorf(err.Error())
	}

	_, err = LoadCommitMsg(store, datastore.NewKey("/bad/path"))
	if err == nil {
		t.Errorf("expected loading a bad path to error. got nil")
		return
	}

	expect := "error loading commit file: datastore: key not found"
	if err.Error() != expect {
		t.Errorf("error mismatch. expected: '%s', got: '%s'", expect, err.Error())
	}
}
