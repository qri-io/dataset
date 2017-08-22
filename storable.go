package dataset

import (
	"fmt"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/castore"
)

var ErrNoPath = fmt.Errorf("missing path")

// storable is the internal interface for anything that can save / load from a
// content content-addressed store
type storable interface {
	Load(store castore.Datastore) error
	Save(store castore.Datastore) (datastore.Key, error)
}
