package dsfs

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// SaveViz saves a query's viz to a given store
func SaveViz(store cafs.Filestore, v *dataset.Viz, pin bool) (path datastore.Key, err error) {
	file, err := JSONFile(PackageFileViz.String(), v)
	if err != nil {
		log.Debug(err.Error())
		return datastore.NewKey(""), fmt.Errorf("error saving json viz file: %s", err.Error())
	}
	return store.Put(file, pin)
}

// LoadViz loads a viz from a given path in a store
func LoadViz(store cafs.Filestore, path datastore.Key) (st *dataset.Viz, err error) {
	path = PackageKeypath(store, path, PackageFileViz)
	return loadViz(store, path)
}

// loadViz assumes the provided path is valid
func loadViz(store cafs.Filestore, path datastore.Key) (st *dataset.Viz, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading viz file: %s", err.Error())
	}
	return dataset.UnmarshalViz(data)
}
