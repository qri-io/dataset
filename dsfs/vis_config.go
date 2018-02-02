package dsfs

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// SaveVisConfig saves a query's visconfig to a given store
func SaveVisConfig(store cafs.Filestore, v *dataset.VisConfig, pin bool) (path datastore.Key, err error) {
	file, err := JSONFile(PackageFileVisConfig.String(), v)
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error saving json visconfig file: %s", err.Error())
	}
	return store.Put(file, pin)
}

// LoadVisConfig loads a visconfig from a given path in a store
func LoadVisConfig(store cafs.Filestore, path datastore.Key) (st *dataset.VisConfig, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		return nil, fmt.Errorf("error loading visconfig file: %s", err.Error())
	}
	return dataset.UnmarshalVisConfig(data)
}
