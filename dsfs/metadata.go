package dsfs

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// SaveMetadata saves a query's metadata to a given store
func SaveMetadata(store cafs.Filestore, s *dataset.Metadata, pin bool) (path datastore.Key, err error) {
	file, err := JSONFile(PackageFileMetadata.String(), s)
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error saving json metadata file: %s", err.Error())
	}
	return store.Put(file, pin)
}

// LoadMetadata loads a metadata from a given path in a store
func LoadMetadata(store cafs.Filestore, path datastore.Key) (md *dataset.Metadata, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		return nil, fmt.Errorf("error loading metadata file: %s", err.Error())
	}
	return dataset.UnmarshalMetadata(data)
}
