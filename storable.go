package dataset

import (
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/castore"
)

// storable is the internal interface for anything that can save / load from a
// content content-addressed store
type storable interface {
	Load(store castore.Datastore, path datastore.Key) error
	Save(store castore.Datastore) (datastore.Key, error)
}
