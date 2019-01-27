package dsfs

import (
	"fmt"

	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// SaveMeta saves a query's metadata to a given store
func SaveMeta(store cafs.Filestore, s *dataset.Meta, pin bool) (path string, err error) {
	file, err := JSONFile(PackageFileMeta.String(), s)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error saving json metadata file: %s", err.Error())
	}
	return store.Put(file, pin)
}

// LoadMeta loads a metadata from a given path in a store
func LoadMeta(store cafs.Filestore, path string) (md *dataset.Meta, err error) {
	path = PackageFilepath(store, path, PackageFileMeta)
	return loadMeta(store, path)
}

// loadMeta assumes the provided path is valid
func loadMeta(store cafs.Filestore, path string) (md *dataset.Meta, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading metadata file: %s", err.Error())
	}
	return dataset.UnmarshalMeta(data)
}
