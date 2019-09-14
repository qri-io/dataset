package dsfs

import (
	"context"
	"testing"

	"github.com/qri-io/qfs/cafs"
)

func TestLoadMeta(t *testing.T) {
	ctx := context.Background()
	store := cafs.NewMapstore()
	a, err := SaveMeta(ctx, store, AirportCodes.Meta, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadMeta(ctx, store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}
