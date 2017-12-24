package dsfs

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// SaveMeta saves a query's metadata to a given store
func SaveMeta(store cafs.Filestore, s *dataset.Meta, pin bool) (path datastore.Key, err error) {
	file, err := JSONFile(PackageFileMeta.String(), s)
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error saving json metadata file: %s", err.Error())
	}
	return store.Put(file, pin)
}

// LoadMeta loads a metadata from a given path in a store
func LoadMeta(store cafs.Filestore, path datastore.Key) (md *dataset.Meta, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		return nil, fmt.Errorf("error loading metadata file: %s", err.Error())
	}
	return dataset.UnmarshalMeta(data)
}
