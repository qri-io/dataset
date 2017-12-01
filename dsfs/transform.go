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

	if q.Structure != nil && !q.Structure.IsEmpty() {
		path, err := SaveStructure(store, q.Structure, pin)
		if err != nil {
			return datastore.NewKey(""), err
		}
		save.Structure = dataset.NewStructureRef(path)
	}

	// absp, err := SaveAbstractTransform(store, save.Abstract, pin)
	// if err != nil {
	// 	return datastore.NewKey(""), fmt.Errorf("error saving abstract transform: %s", err.Error())
	// }
	// save.Abstract = dataset.NewAbstractTransformRef(absp)

	// convert any full datasets to path references
	for name, d := range save.Resources {
		if d.Path().String() != "" && d.IsEmpty() {
			continue
		} else if d != nil {
			save.Resources[name] = dataset.NewDatasetRef(d.Path())
		}
	}

	qdata, err := save.MarshalJSON()
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error marshaling transform data to json: %s", err.Error())
	}

	return store.Put(memfs.NewMemfileBytes(PackageFileTransform.String(), qdata), pin)
}

func transformFile(q *dataset.Transform) (cafs.File, error) {
	if q == nil {
		return nil, nil
	}
	// if !q.Abstract.IsEmpty() {
	// 	return nil, fmt.Errorf("transform abstract transform must be a reference to generate a transform file")
	// }

	// convert any full datasets to path references
	for name, d := range q.Resources {
		if d.Path().String() != "" && d.IsEmpty() {
			continue
		} else if d != nil {
			q.Resources[name] = dataset.NewDatasetRef(d.Path())
		}
	}

	qdata, err := json.Marshal(q)
	if err != nil {
		return nil, fmt.Errorf("error marshaling transform data to json: %s", err.Error())
	}

	return memfs.NewMemfileBytes(PackageFileTransform.String(), qdata), nil
}

// // LoadAbstractTransform loads a transform from a given path in a store
// func LoadAbstractTransform(store cafs.Filestore, path datastore.Key) (q *dataset.AbstractTransform, err error) {
// 	data, err := fileBytes(store.Get(path))
// 	if err != nil {
// 		return nil, fmt.Errorf("error loading transform raw data: %s", err.Error())
// 	}

// 	return dataset.UnmarshalAbstractTransform(data)
// }

// // SaveAbstractTransform writes an AbstractTransform to a cafs
// func SaveAbstractTransform(store cafs.Filestore, q *dataset.AbstractTransform, pin bool) (datastore.Key, error) {
// 	if q == nil {
// 		return datastore.NewKey(""), nil
// 	}

// 	// *don't* need to break transform out into different structs.
// 	// stpath, err := q.Structure.Save(store)
// 	// if err != nil {
// 	//  return datastore.NewKey(""), err
// 	// }

// 	qdata, err := json.Marshal(q)
// 	if err != nil {
// 		return datastore.NewKey(""), fmt.Errorf("error marshaling transform data to json: %s", err.Error())
// 	}

// 	return store.Put(memfs.NewMemfileBytes("transform.json", qdata), pin)
// }
