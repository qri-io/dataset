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

// ErrNoViz is the error for asking a dataset without a viz component for viz info
var ErrNoViz = fmt.Errorf("this dataset has no viz component")

// LoadVizScript loads script data from a dataset path if the given dataset has a viz script is specified
// the returned cafs.File will be the value of dataset.Viz.ScriptPath
func LoadVizScript(store cafs.Filestore, dspath datastore.Key) (cafs.File, error) {
	ds, err := LoadDataset(store, dspath)
	if err != nil {
		return nil, err
	}

	if ds.Viz == nil || ds.Viz.ScriptPath == "" {
		return nil, ErrNoViz
	}

	return store.Get(datastore.NewKey(ds.Viz.ScriptPath))
}
