package dsfs

import (
	"fmt"

	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/fs"
)

// LoadTransform loads a transform from a given path in a store
func LoadTransform(store cafs.Filestore, path string) (q *dataset.Transform, err error) {
	path = PackageFilepath(store, path, PackageFileTransform)
	return loadTransform(store, path)
}

// loadTransform assumes the provided path is correct
func loadTransform(store cafs.Filestore, path string) (q *dataset.Transform, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading transform raw data: %s", err.Error())
	}

	return dataset.UnmarshalTransform(data)
}

// SaveTransform writes a transform to a cafs
func SaveTransform(store cafs.Filestore, q *dataset.Transform, pin bool) (path string, err error) {
	// copy transform
	save := &dataset.Transform{}
	save.Assign(q)
	save.Qri = dataset.KindTransform.String()
	save.DropTransientValues()

	tf, err := JSONFile(PackageFileTransform.String(), save)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error marshaling transform data to json: %s", err.Error())
	}

	return store.Put(tf, pin)
}

// ErrNoTransform is the error for asking a dataset without a tranform component for viz info
var ErrNoTransform = fmt.Errorf("this dataset has no transform component")

// LoadTransformScript loads transform script data from a dataset path if the given dataset has a transform script specified
// the returned cafs.File will be the value of dataset.Transform.ScriptPath
// TODO - this is broken, assumes file is JSON. fix & test or depricate
func LoadTransformScript(store cafs.Filestore, dspath string) (fs.File, error) {
	ds, err := LoadDataset(store, dspath)
	if err != nil {
		return nil, err
	}

	if ds.Transform == nil || ds.Transform.ScriptPath == "" {
		return nil, ErrNoTransform
	}

	return store.Get(ds.Transform.ScriptPath)
}
