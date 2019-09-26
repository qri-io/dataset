package dsfs

import (
	"context"
	"testing"

	"github.com/qri-io/qfs/cafs"
)

func TestLoadStructure(t *testing.T) {
	ctx := context.Background()
	store := cafs.NewMapstore()
	a, err := SaveStructure(ctx, store, AirportCodesStructure)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadStructure(ctx, store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}
