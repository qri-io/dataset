package dsfs

import (
	"context"
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/qfs/cafs"
)

// SaveCommit writes a commit message to a cafs
func SaveCommit(ctx context.Context, store cafs.Filestore, s *dataset.Commit) (path string, err error) {
	file, err := JSONFile(PackageFileCommit.String(), s)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error saving json commit file: %s", err.Error())
	}
	return store.Put(ctx, file)
}

// LoadCommit loads a commit from a given path in a store
func LoadCommit(ctx context.Context, store cafs.Filestore, path string) (st *dataset.Commit, err error) {
	path = PackageFilepath(store, path, PackageFileCommit)
	return loadCommit(ctx, store, path)
}

// loadCommit assumes the provided path is valid
func loadCommit(ctx context.Context, store cafs.Filestore, path string) (st *dataset.Commit, err error) {
	data, err := fileBytes(store.Get(ctx, path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading commit file: %s", err.Error())
	}
	return dataset.UnmarshalCommit(data)
}
