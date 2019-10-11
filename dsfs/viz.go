package dsfs

import (
	"context"
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/qfs"
	"github.com/qri-io/qfs/cafs"
)

// SaveViz saves a query's viz to a given store
func SaveViz(ctx context.Context, store cafs.Filestore, v *dataset.Viz) (path string, err error) {
	file, err := JSONFile(PackageFileViz.String(), v)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error saving json viz file: %s", err.Error())
	}
	return store.Put(ctx, file)
}

// LoadViz loads a viz from a given path in a store
func LoadViz(ctx context.Context, store cafs.Filestore, path string) (st *dataset.Viz, err error) {
	path = PackageFilepath(store, path, PackageFileViz)
	return loadViz(ctx, store, path)
}

// loadViz assumes the provided path is valid
func loadViz(ctx context.Context, store cafs.Filestore, path string) (st *dataset.Viz, err error) {
	data, err := fileBytes(store.Get(ctx, path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading viz file: %s", err.Error())
	}
	return dataset.UnmarshalViz(data)
}

// ErrNoViz is the error for asking a dataset without a viz component for viz info
var ErrNoViz = fmt.Errorf("this dataset has no viz component")

// LoadVizScript loads script data from a dataset path if the given dataset has a viz script is specified
// the returned qfs.File will be the value of dataset.Viz.ScriptPath
func LoadVizScript(ctx context.Context, store cafs.Filestore, dspath string) (qfs.File, error) {
	ds, err := LoadDataset(ctx, store, dspath)
	if err != nil {
		return nil, err
	}

	if ds.Viz == nil || ds.Viz.ScriptPath == "" {
		return nil, ErrNoViz
	}

	return store.Get(ctx, ds.Viz.ScriptPath)
}
