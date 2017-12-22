package dsfs

import (
	"testing"

	"github.com/qri-io/cafs/memfs"
)

func TestLoadMetadata(t *testing.T) {
	store := memfs.NewMapstore()
	a, err := SaveMetadata(store, AirportCodes.Metadata, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadMetadata(store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}
