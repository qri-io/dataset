package dsfs

import (
	"context"
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/qfs/cafs"
)

// SaveMeta saves a query's metadata to a given store
func SaveMeta(ctx context.Context, store cafs.Filestore, s *dataset.Meta) (path string, err error) {
	file, err := JSONFile(PackageFileMeta.String(), s)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error saving json metadata file: %s", err.Error())
	}
	return store.Put(ctx, file)
}

// LoadMeta loads a metadata from a given path in a store
func LoadMeta(ctx context.Context, store cafs.Filestore, path string) (md *dataset.Meta, err error) {
	path = PackageFilepath(store, path, PackageFileMeta)
	return loadMeta(ctx, store, path)
}

// loadMeta assumes the provided path is valid
func loadMeta(ctx context.Context, store cafs.Filestore, path string) (md *dataset.Meta, err error) {
	data, err := fileBytes(store.Get(ctx, path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading metadata file: %s", err.Error())
	}
	return dataset.UnmarshalMeta(data)
}
