package dsfs

import (
	"testing"

	"github.com/qri-io/cafs"
)

func TestLoadStructure(t *testing.T) {
	store := cafs.NewMapstore()
	a, err := SaveStructure(store, AirportCodesStructure, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadStructure(store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}
