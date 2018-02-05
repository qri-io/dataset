package dsfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-crypto"
	"github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/validate"
	"github.com/qri-io/datasetDiffer"
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
	if err := DerefDatasetMeta(store, ds); err != nil {
		return err
	}
	if err := DerefDatasetStructure(store, ds); err != nil {
		return err
	}
	if err := DerefDatasetTransform(store, ds); err != nil {
		return err
	}
	if err := DerefDatasetVisConfig(store, ds); err != nil {
		return err
	}

	return DerefDatasetCommit(store, ds)
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

// DerefDatasetVisConfig derferences a dataset's VisConfig element if required
// should be a no-op if ds.VisConfig is nil or isn't a reference
func DerefDatasetVisConfig(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.VisConfig != nil && ds.VisConfig.IsEmpty() && ds.VisConfig.Path().String() != "" {
		st, err := LoadVisConfig(store, ds.VisConfig.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset visconfig: %s", err.Error())
		}
		// assign path to retain internal reference to path
		st.Assign(dataset.NewVisConfigRef(ds.VisConfig.Path()))
		ds.VisConfig = st
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

// DerefDatasetMeta derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetMeta(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Meta != nil && ds.Meta.IsEmpty() && ds.Meta.Path().String() != "" {
		md, err := LoadMeta(store, ds.Meta.Path())
		if err != nil {
			return fmt.Errorf("error loading dataset metadata: %s", err.Error())
		}
		// assign path to retain internal reference to path
		md.Assign(dataset.NewMetaRef(ds.Meta.Path()))
		ds.Meta = md
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
	// var diffDescription string

	if pk == nil {
		err = fmt.Errorf("private key is required to create a dataset")
		return
	}
	if err = DerefDataset(store, ds); err != nil {
		return
	}
	if err = validate.Dataset(ds); err != nil {
		return
	}
	df, _, err = prepareDataset(store, ds, df, pk)
	if err != nil {
		return
	}
	// if diffDescription == "" {
	// 	err = fmt.Errorf("cannot record changes if no changes occured")
	// 	return
	// }

	// if err = confirmChangesOccurred(store, ds, df); err != nil {
	//   err = fmt.Errorf("cannot record changes if no changes occured")
	// 	 return
	// }
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

func generateCommitMsg(store cafs.Filestore, ds *dataset.Dataset) (string, error) {
	// check for user-supplied commit message
	var prev *dataset.Dataset
	if ds.PreviousPath != "" {
		prevKey := datastore.NewKey(ds.PreviousPath)
		var err error
		prev, err = LoadDataset(store, prevKey)
		if err != nil {
			return "", fmt.Errorf("error loading previous dataset: %s", err.Error())
		}
	} else {
		prev = &dataset.Dataset{
			Commit: &dataset.Commit{},
			Structure: &dataset.Structure{
				Checksum: base58.Encode([]byte(`abc`)),
				Format:   ds.Structure.Format,
			},
			DataPath: "abc",
			// Meta:     nil,
		}
	}

	diffMap, err := datasetDiffer.DiffDatasets(prev, ds, nil)
	if err != nil {
		err = fmt.Errorf("error diffing datasets: %s", err.Error())
		return "", err
	}
	diffDescription, err := datasetDiffer.MapDiffsToString(diffMap, "listKeys")
	if err != nil {
		return "", err
	}
	// ds.Commit.Title = diffDescription
	return diffDescription, nil
}

// prepareDataset modifies a dataset in preparation for adding to a dsfs
// it returns a new data file for use in WriteDataset
func prepareDataset(store cafs.Filestore, ds *dataset.Dataset, df cafs.File, privKey crypto.PrivKey) (cafs.File, string, error) {
	// TODO - need a better strategy for huge files. I think that strategy is to split
	// the reader into multiple consumers that are all performing their task on a stream
	// of byte slices
	var err error
	if df == nil && ds.PreviousPath == "" {
		return nil, "", fmt.Errorf("datafile or dataset PreviousPath needed")
	}
	if df == nil && ds.PreviousPath != "" {
		prev, err := LoadDataset(store, datastore.NewKey(ds.PreviousPath))
		if err != nil {
			return nil, "", fmt.Errorf("error loading previous dataset: %s", err)
		}
		df, err = LoadData(store, prev)
		if err != nil {
			return nil, "", fmt.Errorf("error loading previous dataset data: %s", err)
		}
	}
	data, err := ioutil.ReadAll(df)
	if err != nil {
		return nil, "", fmt.Errorf("error reading file: %s", err.Error())
	}
	ds.Structure.Length = len(data)

	// set error count
	validationErrors := ds.Structure.Schema.ValidateBytes(data)
	ds.Structure.ErrCount = len(validationErrors)

	// TODO - add a dsio.RowCount function that avoids actually arranging data into rows
	rr, err := dsio.NewValueReader(ds.Structure, memfs.NewMemfileBytes("data", data))
	if err != nil {
		return nil, "", fmt.Errorf("error reading data values: %s", err.Error())
	}

	entries := 0
	for err == nil {
		entries++
		_, err = rr.ReadValue()
	}
	if err.Error() != "EOF" {
		return nil, "", fmt.Errorf("error reading values: %s", err.Error())
	}

	ds.Structure.Entries = entries

	// TODO - set hash
	shasum, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		return nil, "", fmt.Errorf("error calculating hash: %s", err.Error())
	}
	ds.Structure.Checksum = shasum.B58String()

	// generate abstract form of dataset
	ds.Abstract = dataset.Abstract(ds)
	//get auto commit message if necessary
	diffDescription, err := generateCommitMsg(store, ds)
	if err != nil {
		return nil, "", fmt.Errorf("%s", err.Error())
	}

	if ds.Commit.Title == "" {
		ds.Commit.Title = diffDescription
	}
	ds.Commit.Timestamp = timestamp()
	signedBytes, err := privKey.Sign(ds.Commit.SignableBytes())
	if err != nil {
		return nil, "", fmt.Errorf("error signing commit title: %s", err.Error())
	}
	ds.Commit.Signature = base58.Encode(signedBytes)

	return memfs.NewMemfileBytes("data."+ds.Structure.Format.String(), data), diffDescription, nil
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

	if ds.Meta != nil {
		mdf, err := JSONFile(PackageFileMeta.String(), ds.Meta)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling metadata to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(mdf)
	}

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

	if ds.VisConfig != nil {
		vc, err := JSONFile(PackageFileVisConfig.String(), ds.VisConfig)
		if err != nil {
			return datastore.NewKey(""), fmt.Errorf("error marshaling dataset visconfig to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(vc)
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
			case PackageFileMeta.String():
				ds.Meta = dataset.NewMetaRef(ao.Path)
			case PackageFileCommit.String():
				ds.Commit = dataset.NewCommitRef(ao.Path)
			case PackageFileVisConfig.String():
				ds.VisConfig = dataset.NewVisConfigRef(ao.Path)
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
