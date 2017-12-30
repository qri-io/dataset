package dsfs

import (
	"testing"

	"github.com/qri-io/cafs/memfs"
)

func TestLoadMeta(t *testing.T) {
	store := memfs.NewMapstore()
	a, err := SaveMeta(store, AirportCodes.Meta, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadMeta(store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}
