package dsfs

import (
	"github.com/qri-io/cafs"
	"testing"
)

func TestLoadStructure(t *testing.T) {
	store := cafs.NewMapstore()
	a, err := AirportCodesStructure.Save(store, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadStructure(store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}
