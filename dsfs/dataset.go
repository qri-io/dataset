package dsfs

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

// LoadDataset reads a dataset from a cafs and dereferences structure, transform, and commitMsg if they exist,
// returning a fully-hydrated dataset
func LoadDataset(store cafs.Filestore, path datastore.Key) (*dataset.Dataset, error) {
	ds, err := LoadDatasetRefs(store, path)
	if err != nil {
		return nil, err
	}

	if err := DerefDatasetStructure(store, ds); err != nil {
		return nil, fmt.Errorf("error dereferencing %s file: %s", PackageFileStructure, err.Error())
	}

	if err := DerefDatasetTransform(store, ds); err != nil {
		return nil, fmt.Errorf("error dereferencing %s file: %s", PackageFileTransform, err.Error())
	}

	if err := DerefDatasetCommitMsg(store, ds); err != nil {
		return nil, fmt.Errorf("error dereferencing %s file: %s", PackageFileTransform, err.Error())
	}

	return ds, nil
}

// LoadDatasetRefs reads a dataset from a content addressed filesystem
func LoadDatasetRefs(store cafs.Filestore, path datastore.Key) (*dataset.Dataset, error) {
	ds := &dataset.Dataset{}

	data, err := fileBytes(store.Get(path))
	// if err != nil {
	// 	return nil, fmt.Errorf("error getting file bytes: %s", err.Error())
	// }

	// TODO - for some reason files are sometimes coming back empty from IPFS,
	// every now & then. In the meantime, let's give a second try if data is empty
	if err != nil || len(data) == 0 {
		data, err = fileBytes(store.Get(path))
		if err != nil {
			return nil, fmt.Errorf("error getting file bytes: %s", err.Error())
		}
	}

	ds, err = dataset.UnmarshalDataset(data)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling %s file: %s", PackageFileDataset.String(), err.Error())
	}

	// assign path to retain internal reference to the
	// path this dataset was read from
	ds.Assign(dataset.NewDatasetRef(path))

	return ds, nil
}

// DerefDatasetStructure derferences a dataset's structure element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetStructure(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Structure != nil && ds.Structure.IsEmpty() && ds.Structure.Path().String() != "" {
		st, err := LoadStructure(store, ds.Structure.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset structure: %s", err.Error())
		}
		ds.Structure = st
	}
	return nil
}

// DerefDatasetTransform derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetTransform(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Transform != nil && ds.Transform.IsEmpty() && ds.Transform.Path().String() != "" {
		q, err := LoadTransform(store, ds.Transform.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset transform: %s", err.Error())
		}
		ds.Transform = q
	}
	return nil
}

// DerefDatasetCommitMsg derferences a dataset's Commit element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetCommitMsg(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Commit != nil && ds.Commit.IsEmpty() && ds.Commit.Path().String() != "" {
		cm, err := LoadCommitMsg(store, ds.Commit.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset commit: %s", err.Error())
		}
		ds.Commit = cm
	}
	return nil
}

// SaveDataset writes a dataset to a cafs, replacing subcomponents of a dataset with hash references
// during the write process. Directory structure is according to PackageFile naming conventions
func SaveDataset(store cafs.Filestore, ds *dataset.Dataset, pin bool) (datastore.Key, error) {
	if ds == nil {
		return datastore.NewKey(""), nil
	}

	fileTasks := 0
	addedDataset := false
	adder, err := store.NewAdder(pin, true)
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error creating new adder: %s", err.Error())
	}

	// if dataset contains no references, place directly in.
	// TODO - this might not constitute a valid dataset. should we be
	// validating datasets in here?
	if ds.Transform == nil && ds.Structure == nil {
		dsdata, err := json.Marshal(ds)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(memfs.NewMemfileBytes(PackageFileDataset.String(), dsdata))
		addedDataset = true
	}

	if ds.Transform != nil {
		// all resources must be references
		for key, r := range ds.Transform.Resources {
			if r.Path().String() == "" {
				return datastore.NewKey(""), fmt.Errorf("transform resource %s requires a path to save", key)
			}
			if !r.IsEmpty() {
				ds.Transform.Resources[key] = dataset.NewDatasetRef(r.Path())
			}
		}
		qdata, err := json.Marshal(ds.Transform)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset transform to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(memfs.NewMemfileBytes(PackageFileTransform.String(), qdata))
	}

	if ds.AbstractTransform != nil {
		// ensure all dataset references are abstract
		for key, r := range ds.AbstractTransform.Resources {
			// ds.AbstractTransform.Resources[key]
			absdata, err := json.Marshal(dataset.Abstract(r))
			if err != nil {
				return datastore.NewKey(""), fmt.Errorf("error marshaling dataset abstract to json: %s", err.Error())
			}

			fileTasks++
			adder.AddFile(memfs.NewMemfileBytes(fmt.Sprintf("%s_abst.json", key), absdata))
		}
		qdata, err := json.Marshal(ds.AbstractTransform)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset abstract transform to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(memfs.NewMemfileBytes(PackageFileAbstractTransform.String(), qdata))
	}

	if ds.Commit != nil {
		ds.Commit.Kind = dataset.KindCommitMsg
		cmdata, err := json.Marshal(ds.Commit)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshilng dataset commit message to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(memfs.NewMemfileBytes(PackageFileCommitMsg.String(), cmdata))
	}

	if ds.Structure != nil {
		stdata, err := json.Marshal(ds.Structure)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset structure to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(memfs.NewMemfileBytes(PackageFileStructure.String(), stdata))

		asdata, err := json.Marshal(dataset.Abstract(ds))
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset abstract to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(memfs.NewMemfileBytes(PackageFileAbstract.String(), asdata))

		data, err := store.Get(datastore.NewKey(ds.Data))
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error getting dataset raw data: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(memfs.NewMemfileReader("data."+ds.Structure.Format.String(), data))
	}

	// if ds.Previous != nil {
	// }

	var path datastore.Key
	done := make(chan error, 0)
	go func() {
		for ao := range adder.Added() {
			path = ao.Path
			switch ao.Name {
			case PackageFileStructure.String():
				ds.Structure = dataset.NewStructureRef(ao.Path)
			case PackageFileAbstract.String():
				ds.Abstract = dataset.NewDatasetRef(ao.Path)
			case PackageFileTransform.String():
				ds.Transform = dataset.NewTransformRef(ao.Path)
			case PackageFileAbstractTransform.String():
				ds.AbstractTransform = dataset.NewTransformRef(ao.Path)
			case PackageFileCommitMsg.String():
				ds.Commit = dataset.NewCommitMsgRef(ao.Path)
			default:
				if ds.AbstractTransform != nil {
					for key := range ds.AbstractTransform.Resources {
						if ao.Name == fmt.Sprintf("%s_abst.json", key) {
							ds.AbstractTransform.Resources[key] = dataset.NewDatasetRef(ao.Path)
						}
					}
				}
			}

			fileTasks--
			if fileTasks == 0 {
				if !addedDataset {
					dsdata, err := json.Marshal(ds)
					if err != nil {
						done <- err
						return
					}

					adder.AddFile(memfs.NewMemfileBytes(PackageFileDataset.String(), dsdata))
				}
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

	// ok, this is a horrible hack to deal with the fact that the location of
	// the actual dataset.json on ipfs will be /[hash]/dataset.json, a property
	// that may or may not apply to other cafs implementations.
	// We want to store the reference to the directory hash, so the
	// /[hash]/dataset.json form is desirable, because we can do stuff like
	// /[hash]/abstract_structure.json, and so on, but it's hard to extract
	// in a clean way. maybe a function that re-extracts this info on either
	// the cafs interface, or the concrete cafs/ipfs implementation?
	// TODO - remove this in favour of some sort of method on filestores
	// that generate path roots
	if store.PathPrefix() == "ipfs" {
		path = datastore.NewKey(path.String() + "/" + PackageFileDataset.String())
	}
	return path, err
}
