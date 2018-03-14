package dsfs

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// SaveCommit writes a commit message to a cafs
func SaveCommit(store cafs.Filestore, s *dataset.Commit, pin bool) (path datastore.Key, err error) {
	file, err := JSONFile(PackageFileCommit.String(), s)
	if err != nil {
		log.Debug(err.Error())
		return datastore.NewKey(""), fmt.Errorf("error saving json commit file: %s", err.Error())
	}
	return store.Put(file, pin)
}

// LoadCommit loads a commit from a given path in a store
func LoadCommit(store cafs.Filestore, path datastore.Key) (st *dataset.Commit, err error) {
	path = PackageKeypath(store, path, PackageFileCommit)
	return loadCommit(store, path)
}

// loadCommit assumes the provided path is valid
func loadCommit(store cafs.Filestore, path datastore.Key) (st *dataset.Commit, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading commit file: %s", err.Error())
	}
	return dataset.UnmarshalCommit(data)
}
