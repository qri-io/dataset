package dsfs

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-datastore"
	// "github.com/libp2p/go-libp2p-crypto"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

// LoadDataset reads a dataset from a cafs and dereferences structure, transform, and commitMsg if they exist,
// returning a fully-hydrated dataset
func LoadDataset(store cafs.Filestore, path datastore.Key) (*dataset.Dataset, error) {
	ds, err := LoadDatasetRefs(store, path)
	if err != nil {
		return nil, fmt.Errorf("error loading dataset: %s", err.Error())
	}

	if err := DerefDatasetMetadata(store, ds); err != nil {
		return nil, err
	}
	if err := DerefDatasetStructure(store, ds); err != nil {
		return nil, err
	}
	if err := DerefDatasetTransform(store, ds); err != nil {
		return nil, err
	}
	if err := DerefDatasetCommit(store, ds); err != nil {
		return nil, err
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
		// assign path to retain internal reference to path
		st.Assign(dataset.NewStructureRef(ds.Structure.Path()))
		ds.Structure = st
	}
	return nil
}

// DerefDatasetTransform derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetTransform(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Transform != nil && ds.Transform.IsEmpty() && ds.Transform.Path().String() != "" {
		t, err := LoadTransform(store, ds.Transform.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset transform: %s", err.Error())
		}
		// assign path to retain internal reference to path
		t.Assign(dataset.NewTransformRef(ds.Transform.Path()))
		ds.Transform = t
	}
	return nil
}

// DerefDatasetMetadata derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetMetadata(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Metadata != nil && ds.Metadata.IsEmpty() && ds.Metadata.Path().String() != "" {
		md, err := LoadMetadata(store, ds.Metadata.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset metadata: %s", err.Error())
		}
		// assign path to retain internal reference to path
		md.Assign(dataset.NewMetadataRef(ds.Metadata.Path()))
		ds.Metadata = md
	}
	return nil
}

// DerefDatasetCommit derferences a dataset's Commit element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetCommit(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Commit != nil && ds.Commit.IsEmpty() && ds.Commit.Path().String() != "" {
		cm, err := LoadCommit(store, ds.Commit.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset commit: %s", err.Error())
		}
		// assign path to retain internal reference to path
		cm.Assign(dataset.NewCommitRef(ds.Commit.Path()))
		ds.Commit = cm
	}
	return nil
}

// CreateDatasetParams defines parmeters for the CreateDataset function
// type CreateDatasetParams struct {
// 	// Store is where we're going to
// 	Store cafs.Filestore
// 	//
// 	Dataset  *dataset.Dataset
// 	DataFile cafs.File
// 	PrivKey  crypto.PrivKey
// }

// CreateDataset is the canonical method for getting a dataset pointer & it's data into a store
// func CreateDataset(p *CreateDatasetParams) (path datastore.Key, err error) {
// 	// TODO - need a better strategy for huge files
// 	data, err := ioutil.ReadAll(rdr)
// 	if err != nil {
// 		return fmt.Errorf("error reading file: %s", err.Error())
// 	}

// 	if err = PrepareDataset(p.Store, p.Dataset, p.DataFile); err != nil {
// 		return
// 	}

// 	// Ensure that dataset is well-formed
// 	// format, err := detect.ExtensionDataFormat(filename)
// 	// if err != nil {
// 	// 	return fmt.Errorf("error detecting format extension: %s", err.Error())
// 	// }
// 	// if err = validate.DataFormat(format, bytes.NewReader(data)); err != nil {
// 	// 	return fmt.Errorf("invalid data format: %s", err.Error())
// 	// }

// 	// TODO - check for errors in dataset and warn user if errors exist

// 	datakey, err := store.Put(memfs.NewMemfileBytes("data."+st.Format.String(), data), false)
// 	if err != nil {
// 		return fmt.Errorf("error putting data file in store: %s", err.Error())
// 	}

// 	ds.Timestamp = time.Now().In(time.UTC)
// 	if ds.Title == "" {
// 		ds.Title = name
// 	}
// 	ds.Data = datakey.String()

// 	if err := validate.Dataset(ds); err != nil {
// 		return err
// 	}

// 	dskey, err := SaveDataset(store, ds, true)
// 	if err != nil {
// 		return fmt.Errorf("error saving dataset: %s", err.Error())
// 	}
// }

// prepareDataset modifies a dataset in preparation for adding to a dsfs
// func PrepareDataset(store cafs.Filestore, ds *dataset.Dataset, data cafs.File) error {

// 	st, err := detect.FromReader(data.FileName(), data)
// 	if err != nil {
// 		return fmt.Errorf("error determining dataset schema: %s", err.Error())
// 	}
// 	if ds.Structure == nil {
// 		ds.Structure = &dataset.Structure{}
// 	}
// 	ds.Structure.Assign(st, ds.Structure)

// 	// Ensure that dataset contains valid field names
// 	if err = validate.Structure(st); err != nil {
// 		return fmt.Errorf("invalid structure: %s", err.Error())
// 	}
// 	if err := validate.DataFormat(st.Format, bytes.NewReader(data)); err != nil {
// 		return fmt.Errorf("invalid data format: %s", err.Error())
// 	}

// 	// generate abstract form of dataset
// 	ds.Abstract = dataset.Abstract(ds)

// 	if ds.AbstractTransform != nil {
// 		// convert abstract transform to abstract references
// 		for name, ref := range ds.AbstractTransform.Resources {
// 			// data, _ := ref.MarshalJSON()
// 			// fmt.Println(string(data))
// 			if ref.Abstract != nil {
// 				ds.AbstractTransform.Resources[name] = ref.Abstract
// 			} else {

// 				absf, err := JSONFile(PackageFileAbstract.String(), dataset.Abstract(ref))
// 				if err != nil {
// 					return err
// 				}
// 				path, err := store.Put(absf, true)
// 				if err != nil {
// 					return err
// 				}
// 				ds.AbstractTransform.Resources[name] = dataset.NewDatasetRef(path)
// 			}
// 		}
// 	}

// 	return nil
// }

// SaveDataset writes a dataset to a cafs, replacing subcomponents of a dataset with hash references
// during the write process. Directory structure is according to PackageFile naming conventions
func SaveDataset(store cafs.Filestore, ds *dataset.Dataset, pin bool) (datastore.Key, error) {
	// assign to a new dataset instance to avoid clobbering input dataset
	cp := &dataset.Dataset{}
	cp.Assign(ds)
	ds = cp

	if ds.IsEmpty() {
		return datastore.NewKey(""), fmt.Errorf("cannot save empty dataset")
	}

	fileTasks := 0
	addedDataset := false
	adder, err := store.NewAdder(pin, true)
	if err != nil {
		return datastore.NewKey(""), fmt.Errorf("error creating new adder: %s", err.Error())
	}

	if ds.AbstractTransform != nil {
		// ensure all dataset references are abstract
		for key, r := range ds.AbstractTransform.Resources {
			if !r.IsEmpty() {
				return datastore.NewKey(""), fmt.Errorf("abstract transform resource '%s' is not a reference", key)
			}
		}
		abstff, err := JSONFile(PackageFileAbstractTransform.String(), ds.AbstractTransform)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset abstract transform to json: %s", err.Error())
		}

		fileTasks++
		adder.AddFile(abstff)
	}

	if ds.Metadata != nil {
		mdf, err := JSONFile(PackageFileMetadata.String(), ds.Metadata)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling metadata to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(mdf)
	}

	// if dataset contains no references, place directly in.
	// TODO - this might not constitute a valid dataset. should we be
	// validating datasets in here?
	// if ds.Transform == nil && ds.Structure == nil {
	// 	dsf, err := JSONFile(PackageFileDataset.String(), ds)
	// 	if err != nil {
	// 		return datastore.NewKey(""), fmt.Errorf("error marshaling dataset to json: %s", err.Error())
	// 	}

	// 	fileTasks++
	// 	adder.AddFile(dsf)
	// 	addedDataset = true
	// }

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

	if ds.Commit != nil {
		cmf, err := JSONFile(PackageFileCommit.String(), ds.Commit)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshilng dataset commit message to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(cmf)
	}

	if ds.Structure != nil {
		stf, err := JSONFile(PackageFileStructure.String(), ds.Structure)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset structure to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(stf)
	}

	if ds.Abstract != nil {
		abf, err := JSONFile(PackageFileAbstract.String(), ds.Abstract)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset abstract to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(abf)
	}

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
			case PackageFileMetadata.String():
				ds.Metadata = dataset.NewMetadataRef(ao.Path)
			case PackageFileCommit.String():
				ds.Commit = dataset.NewCommitRef(ao.Path)
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
