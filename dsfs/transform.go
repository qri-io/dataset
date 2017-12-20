package dsfs

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

// LoadTransform loads a transform from a given path in a store
func LoadTransform(store cafs.Filestore, path datastore.Key) (q *dataset.Transform, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		return nil, fmt.Errorf("error loading transform raw data: %s", err.Error())
	}

	return dataset.UnmarshalTransform(data)
}

// SaveTransform writes a transform to a cafs
func SaveTransform(store cafs.Filestore, q *dataset.Transform, pin bool) (path datastore.Key, err error) {
	// copy transform
	save := &dataset.Transform{}
	save.Assign(q)
	save.Kind = dataset.KindTransform

	if q.Structure != nil && !q.Structure.IsEmpty() {
		path, err := SaveStructure(store, q.Structure, pin)
		if err != nil {
			return datastore.NewKey(""), err
		}
		save.Structure = dataset.NewStructureRef(path)
	}

	// convert any full datasets to path references
	for name, d := range save.Resources {
		if d.Path().String() != "" && d.IsEmpty() {
			continue
		} else if d != nil {
			save.Resources[name] = dataset.NewDatasetRef(d.Path())
		}
	}

	tf, err := JSONFile(PackageFileTransform.String(), save)
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error marshaling transform data to json: %s", err.Error())
	}

	return store.Put(tf, pin)
}

// SaveAbstractTransform writes a transform to a cafs, ensuring only it's abstract form is written
func SaveAbstractTransform(store cafs.Filestore, t *dataset.Transform, pin bool) (path datastore.Key, err error) {
	// copy transform
	save := &dataset.Transform{}
	save.Assign(t)
	save.Kind = dataset.KindTransform

	if save.Structure == nil {
		return datastore.NewKey(""), fmt.Errorf("structure required to save abstract transform")
	}

	save.Structure = save.Structure.Abstract()
	stpath, err := SaveStructure(store, save.Structure, pin)
	if err != nil {
		return datastore.NewKey(""), err
	}
	save.Structure = dataset.NewStructureRef(stpath)

	// ensure all dataset references are abstract
	for key, r := range save.Resources {
		absdata, err := json.Marshal(dataset.Abstract(r))
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset abstract to json: %s", err.Error())
		}

		path, err := store.Put(memfs.NewMemfileBytes(fmt.Sprintf("%s_abst.json", key), absdata), pin)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error placing abstract dataset '%s' in store: %s", key, err.Error())
		}

		save.Resources[key] = dataset.NewDatasetRef(path)
	}

	data, err := json.Marshal(save)
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error marshaling dataset abstract transform to json: %s", err.Error())
	}

	return store.Put(memfs.NewMemfileBytes(PackageFileAbstractTransform.String(), data), pin)
}
