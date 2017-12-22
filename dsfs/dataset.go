package dsfs

import (
	"encoding/json"
	"fmt"
	// "io/ioutil"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-crypto"
	"github.com/mr-tron/base58/base58"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/validate"
)

// LoadDataset reads a dataset from a cafs and dereferences structure, transform, and commitMsg if they exist,
// returning a fully-hydrated dataset
func LoadDataset(store cafs.Filestore, path datastore.Key) (*dataset.Dataset, error) {
	ds, err := LoadDatasetRefs(store, path)
	if err != nil {
		return nil, fmt.Errorf("error loading dataset: %s", err.Error())
	}
	if err := DerefDataset(store, ds); err != nil {
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

// DerefDataset attempts to fully dereference a dataset
func DerefDataset(store cafs.Filestore, ds *dataset.Dataset) error {
	if err := DerefDatasetMetadata(store, ds); err != nil {
		return err
	}
	if err := DerefDatasetStructure(store, ds); err != nil {
		return err
	}
	if err := DerefDatasetTransform(store, ds); err != nil {
		return err
	}
	if err := DerefDatasetCommit(store, ds); err != nil {
		return err
	}
	return nil
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

// CreateDataset places a new dataset in the store. Admittedly, this isn't a simple process.
// Store is where we're going to
// Dataset to be saved
// Pin the dataset if the underlying store supports the pinning interface
func CreateDataset(store cafs.Filestore, ds *dataset.Dataset, df cafs.File, pk crypto.PrivKey, pin bool) (path datastore.Key, err error) {
	if err = DerefDataset(store, ds); err != nil {
		return
	}
	if err = validate.Dataset(ds); err != nil {
		return
	}
	if err = prepareDataset(store, ds, df, pk); err != nil {
		return
	}
	path, err = WriteDataset(store, ds, df, pin)
	if err != nil {
		err = fmt.Errorf("error writing dataset: %s", err.Error())
	}
	return
}

// timestamp is a function for getting commit timestamps
// we replace this with a static function for testing purposes
var timestamp = func() time.Time {
	return time.Now()
}

// prepareDataset modifies a dataset in preparation for adding to a dsfs
func prepareDataset(store cafs.Filestore, ds *dataset.Dataset, df cafs.File, privKey crypto.PrivKey) error {
	if df == nil {
		return fmt.Errorf("data file is required")
	}

	// TODO - need a better strategy for huge files. I think that strategy is to split
	// the reader into multiple consumers that are all performing their task on a stream
	// of byte slices
	// data, err := ioutil.ReadAll(df)
	// if err != nil {
	// 	return fmt.Errorf("error reading file: %s", err.Error())
	// }

	// generate abstract form of dataset
	ds.Abstract = dataset.Abstract(ds)

	// datakey, err := store.Put(memfs.NewMemfileBytes("data."+ds.Structure.Format.String(), data), false)
	// if err != nil {
	// 	return fmt.Errorf("error putting data file in store: %s", err.Error())
	// }

	ds.Commit.Timestamp = timestamp()
	signedBytes, err := privKey.Sign(ds.Commit.SignableBytes())
	if err != nil {
		return fmt.Errorf("error signing commit title: %s", err.Error())
	}
	ds.Commit.Signature = base58.Encode(signedBytes)

	// TODO - make sure file ending matches
	// "data."+ds.Structure.Format.String()
	return nil
}

// WriteDataset writes a dataset to a cafs, replacing subcomponents of a dataset with path references
// during the write process. Directory structure is according to PackageFile naming conventions.
// This method is currently exported, but 99% of use cases should use CreateDataset instead of this
// lower-level function
func WriteDataset(store cafs.Filestore, ds *dataset.Dataset, dataFile cafs.File, pin bool) (datastore.Key, error) {
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
		// convert abstract transform to abstract references
		for name, ref := range ds.AbstractTransform.Resources {
			absrf, err := JSONFile(fmt.Sprintf("ref_%s.json", name), dataset.Abstract(ref))
			if err != nil {
				return datastore.NewKey(""), fmt.Errorf("error marshaling dataset resource '%s' to json: %s", name, err.Error())
			}
			fileTasks++
			adder.AddFile(absrf)
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
	// data, err := store.Get(datastore.NewKey(ds.Data))
	// if err != nil {
	// 	return datastore.NewKey(""), fmt.Errorf("error getting dataset raw data: %s", err.Error())
	// }

	fileTasks++
	adder.AddFile(dataFile)

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
			case dataFile.FileName():
				ds.DataPath = ao.Path.String()
			default:
				if ds.AbstractTransform != nil {
					for name := range ds.AbstractTransform.Resources {
						if ao.Name == fmt.Sprintf("ref_%s.json", name) {
							ds.AbstractTransform.Resources[name] = dataset.NewDatasetRef(ao.Path)
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
