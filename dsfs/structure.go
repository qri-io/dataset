package dsfs

import (
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// LoadStructure loads a structure from a given path in a store
func LoadStructure(store cafs.Filestore, path datastore.Key) (st *dataset.Structure, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		return nil, err
	}

	return dataset.UnmarshalStructure(data)
}
