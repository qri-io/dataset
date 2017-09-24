package dsfs

import (
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipfs/commands/files"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfile"
	"github.com/qri-io/dataset"
	"path/filepath"
)

// LoadDatasetData loads the data this dataset points to from the store
func LoadDatasetData(store cafs.Filestore, ds *dataset.Dataset) (files.File, error) {
	return store.Get(ds.Data)
}

// Load a dataset from a cafs. It's assumed that the dataset path will be
// to the package. this func will first try ds.path + "/dataset.json"
// before trying the raw path.
func LoadDataset(store cafs.Filestore, path datastore.Key) (*dataset.Dataset, error) {
	ds := &dataset.Dataset{}
	datasetFilePath := datastore.NewKey(filepath.Join(path.String(), PackageFileDataset.Filename()))

	// fmt.Println(ds.path)
	data, err := fileBytes(store.Get(datasetFilePath))
	if err != nil {
		return nil, err
	}

	ds, err = dataset.UnmarshalDataset(data)
	if err != nil {
		return nil, err
	}

	if ds.Structure != nil && ds.Structure.Path().String() != "" {
		ds.Structure, err = LoadStructure(store, path)
		if err := ds.Structure.Load(store); err != nil {
			return nil, fmt.Errorf("error loading dataset structure: %s", err.Error())
		}
	}

	if ds.Query != nil && ds.Query.Path().String() != "" {
		if err := ds.Query.Load(store); err != nil {
			return nil, fmt.Errorf("error loading dataset query: %s", err.Error())
		}
	}

	// TODO - decide if we should load resource datasets by default, probably shouldn't
	// for _, d := range ds.Resources {
	// 	if d.Path().String() != "" && d.IsEmpty() {
	// 		continue
	// 	} else if d != nil {
	// 		if err := LoadDataset(store); err != nil {
	// 			return nil, fmt.Errorf("error loading dataset resource: %s", err.Error())
	// 		}
	// 	}
	// }
	return ds, nil
}

func SaveDataset(store cafs.Filestore, ds *dataset.Dataset, pin bool) (datastore.Key, error) {
	if ds == nil {
		return datastore.NewKey(""), nil
	}

	fileTasks := 0
	adder, err := store.NewAdder(pin, true)
	if err != nil {
		return datastore.NewKey(""), err
	}

	if ds.Query != nil {
		fileTasks++
		qdata, err := json.Marshal(ds.Query)
		if err != nil {
			return datastore.NewKey(""), err
		}
		adder.AddFile(memfile.NewMemfileBytes("query.json", qdata))
	}

	if ds.Structure != nil {
		fileTasks++
		stdata, err := json.Marshal(ds.Structure)
		if err != nil {
			return datastore.NewKey(""), err
		}
		adder.AddFile(memfile.NewMemfileBytes("structure.json", stdata))

		fileTasks++
		data, err := store.Get(ds.Data)
		if err != nil {
			return datastore.NewKey(""), err
		}
		adder.AddFile(memfile.NewMemfileReader("data."+ds.Structure.Format.String(), data))
	}

	// if ds.Previous != nil {
	// }

	// for name, d := range ds.Resources {
	//  if d.path.String() != "" && d.IsEmpty() {
	//    continue
	//  } else if d != nil {
	//    // dspath, err := d.Save(store, pin)
	//    // if err != nil {
	//    //  return datastore.NewKey(""), fmt.Errorf("error saving dataset resource: %s", err.Error())
	//    // }
	//    // ds.Resources[name] = &Dataset{path: dspath}
	//  }
	// }

	var path datastore.Key
	done := make(chan error, 0)
	go func() {
		for ao := range adder.Added() {
			// fmt.Println(fileTasks, ao)
			path = ao.Path
			switch ao.Name {
			case PackageFileStructure.String():
				ds.Structure = dataset.NewStructureRef(ao.Path)
			case PackageFileQuery.String():
				ds.Query = dataset.NewQueryRef(ao.Path)
			case "resources":
			}

			fileTasks--
			if fileTasks == 0 {
				dsdata, err := json.Marshal(ds)
				if err != nil {
					done <- err
					return
				}

				adder.AddFile(memfile.NewMemfileBytes("dataset.json", dsdata))
				//
				if err := adder.Close(); err != nil {
					done <- err
					return
				}
			}
		}
		done <- nil
	}()

	err = <-done
	return path, err
}
