package dsfs

import (
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// LoadStructure loads a structure from a given path in a store
func LoadStructure(store cafs.Filestore, path datastore.Key) (st *dataset.Structure, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		return nil, err
	}

	return dataset.UnmarshalStructure(data)
}

func SaveStructure(store cafs.Filestore, s *dataset.Structure, pin bool) (path datastore.Key, err error) {
	file, err := jsonFile(PackageFileStructure.String(), s)
	if err != nil {
		return datastore.NewKey(""), err
	}
	return store.Put(file, pin)
}
