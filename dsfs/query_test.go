package dsfs

import (
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"testing"
)

func TestLoadQuery(t *testing.T) {
	store := cafs.NewMapstore()
	q := &dataset.Query{Statement: "select * from whatever booooooo go home"}
	a, err := SaveQuery(store, q, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err = LoadQuery(store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}

func TestQueryLoadAbstractStructures(t *testing.T) {
	// store := datastore.NewMapDatastore()
	// TODO - finish dis test
}
