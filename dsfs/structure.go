package dsfs

import (
	"context"
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/qfs/cafs"
)

// SaveStructure saves a query's structure to a given store
func SaveStructure(ctx context.Context, store cafs.Filestore, s *dataset.Structure) (path string, err error) {
	file, err := JSONFile(PackageFileStructure.String(), s)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error saving json structure file: %s", err.Error())
	}
	return store.Put(ctx, file)
}

// LoadStructure loads a structure from a given path in a store
func LoadStructure(ctx context.Context, store cafs.Filestore, path string) (st *dataset.Structure, err error) {
	path = PackageFilepath(store, path, PackageFileStructure)
	return loadStructure(ctx, store, path)
}

// loadStructure assumes path is valid
func loadStructure(ctx context.Context, store cafs.Filestore, path string) (st *dataset.Structure, err error) {
	data, err := fileBytes(store.Get(ctx, path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading structure file: %s", err.Error())
	}
	return dataset.UnmarshalStructure(data)
}
