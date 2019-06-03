package dsfs

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	crypto "github.com/libp2p/go-libp2p-crypto"
	"github.com/multiformats/go-multihash"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsdiff"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/dsviz"
	"github.com/qri-io/dataset/validate"
	"github.com/qri-io/qfs"
	"github.com/qri-io/qfs/cafs"
)

// LoadDataset reads a dataset from a cafs and dereferences structure, transform, and commitMsg if they exist,
// returning a fully-hydrated dataset
func LoadDataset(store cafs.Filestore, path string) (*dataset.Dataset, error) {
	ds, err := LoadDatasetRefs(store, path)
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading dataset: %s", err.Error())
	}
	if err := DerefDataset(store, ds); err != nil {
		log.Debug(err.Error())
		return nil, err
	}

	return ds, nil
}

// LoadDatasetRefs reads a dataset from a content addressed filesystem without dereferencing
// it's components
func LoadDatasetRefs(store cafs.Filestore, path string) (*dataset.Dataset, error) {
	ds := dataset.NewDatasetRef(path)

	pathWithBasename := PackageFilepath(store, path, PackageFileDataset)
	data, err := fileBytes(store.Get(pathWithBasename))
	// if err != nil {
	// 	return nil, fmt.Errorf("error getting file bytes: %s", err.Error())
	// }

	// TODO - for some reason files are sometimes coming back empty from IPFS,
	// every now & then. In the meantime, let's give a second try if data is empty
	if err != nil || len(data) == 0 {
		data, err = fileBytes(store.Get(pathWithBasename))
		if err != nil {
			log.Debug(err.Error())
			return nil, fmt.Errorf("error getting file bytes: %s", err.Error())
		}
	}

	ds, err = dataset.UnmarshalDataset(data)
	if err != nil {
		log.Debug(err.Error())
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
	if err := DerefDatasetViz(store, ds); err != nil {
		return err
	}
	return DerefDatasetCommit(store, ds)
}

// DerefDatasetStructure derferences a dataset's structure element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetStructure(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Structure != nil && ds.Structure.IsEmpty() && ds.Structure.Path != "" {
		st, err := loadStructure(store, ds.Structure.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset structure: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// st.Assign(dataset.NewStructureRef(ds.Structure.Path))
		ds.Structure = st
	}
	return nil
}

// DerefDatasetViz dereferences a dataset's Viz element if required
// should be a no-op if ds.Viz is nil or isn't a reference
func DerefDatasetViz(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Viz != nil && ds.Viz.IsEmpty() && ds.Viz.Path != "" {
		st, err := loadViz(store, ds.Viz.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset viz: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// st.Assign(dataset.NewVizRef(ds.Viz.Path))
		ds.Viz = st
	}
	return nil
}

// DerefDatasetTransform derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetTransform(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Transform != nil && ds.Transform.IsEmpty() && ds.Transform.Path != "" {
		t, err := loadTransform(store, ds.Transform.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset transform: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// t.Assign(dataset.NewTransformRef(ds.Transform.Path))
		ds.Transform = t
	}
	return nil
}

// DerefDatasetMeta derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetMeta(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Meta != nil && ds.Meta.IsEmpty() && ds.Meta.Path != "" {
		md, err := loadMeta(store, ds.Meta.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset metadata: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// md.Assign(dataset.NewMetaRef(ds.Meta.Path))
		ds.Meta = md
	}
	return nil
}

// DerefDatasetCommit derferences a dataset's Commit element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetCommit(store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Commit != nil && ds.Commit.IsEmpty() && ds.Commit.Path != "" {
		cm, err := loadCommit(store, ds.Commit.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset commit: %s", err.Error())
		}
		// assign path to retain internal reference to path
		cm.Assign(dataset.NewCommitRef(ds.Commit.Path))
		ds.Commit = cm
	}
	return nil
}

// CreateDataset places a new dataset in the store. Admittedly, this isn't a simple process.
// Store is where we're going to
// Dataset to be saved
// Pin the dataset if the underlying store supports the pinning interface
// All streaming files (Body, Transform Script, Viz Script) Must be Resolved before calling if data their data is to be saved
func CreateDataset(store cafs.Filestore, ds, dsPrev *dataset.Dataset, pk crypto.PrivKey, pin, force, shouldRender bool) (path string, err error) {

	if pk == nil {
		err = fmt.Errorf("private key is required to create a dataset")
		return
	}
	if err = DerefDataset(store, ds); err != nil {
		log.Debug(err.Error())
		return
	}
	if err = validate.Dataset(ds); err != nil {
		log.Debug(err.Error())
		return
	}

	if dsPrev != nil && !dsPrev.IsEmpty() {
		if err = DerefDataset(store, dsPrev); err != nil {
			log.Debug(err.Error())
			return
		}
		if err = validate.Dataset(dsPrev); err != nil {
			log.Debug(err.Error())
			return
		}
	}
	_, err = prepareDataset(store, ds, dsPrev, pk, force, shouldRender)
	if err != nil {
		log.Debug(err.Error())
		return
	}

	path, err = WriteDataset(store, ds, pin)
	if err != nil {
		log.Debug(err.Error())
		err = fmt.Errorf("error writing dataset: %s", err.Error())
	}
	return
}

// Timestamp is an function for getting commit timestamps
// timestamps MUST be stored in UTC time zone
var Timestamp = func() time.Time {
	return time.Now().UTC()
}

// prepareDataset modifies a dataset in preparation for adding to a dsfs
// it returns a new data file for use in WriteDataset
func prepareDataset(store cafs.Filestore, ds, dsPrev *dataset.Dataset, privKey crypto.PrivKey, force, shouldRender bool) (string, error) {
	var (
		err error
		// lock for parallel edits to ds pointer
		mu sync.Mutex
		// accumulate reader into a buffer for shasum calculation & passing out another qfs.File
		buf    bytes.Buffer
		bf     = ds.BodyFile()
		bfPrev qfs.File
	)

	if dsPrev != nil {
		bfPrev = dsPrev.BodyFile()
	}

	if bf == nil && bfPrev == nil {
		return "", fmt.Errorf("bodyfile or previous bodyfile needed")
	}

	if bf == nil {
		bf = bfPrev
	}

	errR, errW := io.Pipe()
	entryR, entryW := io.Pipe()
	hashR, hashW := io.Pipe()
	done := make(chan error)
	tasks := 3

	go setErrCount(ds, qfs.NewMemfileReader(bf.FileName(), errR), &mu, done)
	go setDepthAndEntryCount(ds, qfs.NewMemfileReader(bf.FileName(), entryR), &mu, done)
	go setChecksumAndStats(ds, qfs.NewMemfileReader(bf.FileName(), hashR), &buf, &mu, done)

	go func() {
		// pipes must be manually closed to trigger EOF
		defer errW.Close()
		defer entryW.Close()
		defer hashW.Close()

		// allocate a multiwriter that writes to each pipe when
		// mw.Write() is called
		mw := io.MultiWriter(errW, entryW, hashW)
		// copy file bytes to multiwriter from input file
		io.Copy(mw, bf)
	}()

	for i := 0; i < tasks; i++ {
		if err := <-done; err != nil {
			return "", err
		}
	}

	// TODO (ramfox): This whole section can be wrapped:
	// func generateCommit(ds, prev *dataset.Dataset, privKey crypto.PrivKey) error
	// Lots of stuff happening in prepareDataset and the steps to creating the
	// proper commit can be abstracted out
	diffDescription, err := generateCommitMsg(ds, dsPrev, force)
	if err != nil {
		log.Debug(fmt.Errorf("error saving: %s", err))
		return "", fmt.Errorf("error saving: %s", err)
	}

	cleanTitleAndMessage(&ds.Commit.Title, &ds.Commit.Message, diffDescription)

	// TODO (b5): we should check the delta between versions for meaninful changes here,
	// ignoring fields we know will change every time. Can only do this with a proper set
	// of change deltas

	ds.Commit.Timestamp = Timestamp()
	sb, _ := ds.SignableBytes()
	signedBytes, err := privKey.Sign(sb)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error signing commit title: %s", err.Error())
	}
	ds.Commit.Signature = base64.StdEncoding.EncodeToString(signedBytes)
	ds.SetBodyFile(qfs.NewMemfileBytes("body."+ds.Structure.Format, buf.Bytes()))

	if shouldRender && ds.Viz != nil && ds.Viz.ScriptFile() != nil {
		// render the viz
		renderedFile, err := dsviz.Render(ds)
		if err != nil {
			log.Debug(err.Error())
			return "", fmt.Errorf("error rendering visualization: %s", err.Error())
		}
		ds.Viz.SetRenderedFile(renderedFile)
	}

	return diffDescription, nil
}

// setErrCount consumes sets the ErrCount field of a dataset's Structure
func setErrCount(ds *dataset.Dataset, data qfs.File, mu *sync.Mutex, done chan error) {
	defer data.Close()

	er, err := dsio.NewEntryReader(ds.Structure, data)
	if err != nil {
		log.Debug(err.Error())
		done <- fmt.Errorf("reading data values: %s", err.Error())
		return
	}

	validationErrors, err := validate.EntryReader(er)
	if err != nil {
		log.Debug(err.Error())
		done <- fmt.Errorf("validating data: %s", err.Error())
		return
	}

	if ds.Structure != nil {
		if ds.Structure.Strict && len(validationErrors) > 0 {
			done <- fmt.Errorf("strict dataset body is invalid")
			return
		}
	}

	mu.Lock()
	ds.Structure.ErrCount = len(validationErrors)
	mu.Unlock()

	done <- nil
}

// setDepthAndEntryCount set the Entries field of a ds.Structure
func setDepthAndEntryCount(ds *dataset.Dataset, data qfs.File, mu *sync.Mutex, done chan error) {
	defer data.Close()

	er, err := dsio.NewEntryReader(ds.Structure, data)
	if err != nil {
		log.Debug(err.Error())
		done <- fmt.Errorf("error reading data values: %s", err.Error())
		return
	}

	entries := 0
	// baseline of 1 for the original closure
	depth := 1
	var ent dsio.Entry
	for {
		if ent, err = er.ReadEntry(); err != nil {
			log.Debug(err.Error())
			break
		}
		// get the depth of this entry, update depth if larger
		if d := getDepth(ent.Value, 1); d > depth {
			depth = d
		}
		entries++
	}
	if err.Error() != "EOF" {
		done <- fmt.Errorf("error reading values at entry %d: %s", entries, err.Error())
		return
	}

	mu.Lock()
	ds.Structure.Entries = entries
	ds.Structure.Depth = depth
	mu.Unlock()

	done <- nil
}

// getDepth finds the deepest value in a given interface value
func getDepth(x interface{}, depth int) int {
	switch v := x.(type) {
	case map[string]interface{}:
		depth++
		for _, el := range v {
			if d := getDepth(el, depth); d > depth {
				depth = d
			}
		}
	case []interface{}:
		depth++
		for _, el := range v {
			if d := getDepth(el, depth); d > depth {
				depth = d
			}
		}
	}

	return depth
}

// setChecksumAndStats
func setChecksumAndStats(ds *dataset.Dataset, data qfs.File, buf *bytes.Buffer, mu *sync.Mutex, done chan error) {
	defer data.Close()

	if _, err := io.Copy(buf, data); err != nil {
		done <- err
		return
	}

	shasum, err := multihash.Sum(buf.Bytes(), multihash.SHA2_256, -1)
	if err != nil {
		log.Debug(err.Error())
		done <- fmt.Errorf("error calculating hash: %s", err.Error())
		return
	}

	mu.Lock()
	ds.Structure.Checksum = shasum.B58String()
	ds.Structure.Length = len(buf.Bytes())
	mu.Unlock()

	done <- nil
}

// returns a commit message based on the diff of the two datasets
// if there is no previous dataset, it returns "created dataset"
// if there is no difference, the func returns an error
func generateCommitMsg(ds, prev *dataset.Dataset, force bool) (string, error) {
	if prev == nil || prev.IsEmpty() {
		return "created dataset", nil
	}

	diffMap, err := dsdiff.DiffDatasets(prev, ds, nil)
	if err != nil {
		err = fmt.Errorf("error diffing datasets: %s", err.Error())
		return "", err
	}

	diffDescription, err := dsdiff.MapDiffsToString(diffMap, "listKeys")
	if err != nil {
		return "", err
	}

	if diffDescription == "" {
		if force {
			return "forced update", nil
		}
		return "", fmt.Errorf("no changes detected")
	}

	return diffDescription, nil
}

// cleanTitleAndMessage adjusts the title to include no more
// than 70 characters and no more than one line.  Text following
// a line break or this limit will be prepended to the message
func cleanTitleAndMessage(sTitle, sMsg *string, diffDescription string) {
	st := *sTitle
	sm := *sMsg
	if st == "" && diffDescription != "" {
		st = diffDescription
	} else if st == "" {
		// if title is *still* blank move pass message up to title
		st = sm
		sm = ""
	}
	//adjust for length
	if len(st) > 70 {
		cutIndex := 66
		lastSpaceIndex := strings.LastIndex(st[:67], " ")
		if lastSpaceIndex > 0 {
			cutIndex = lastSpaceIndex + 1
		}
		if sm == "" {
			sm = fmt.Sprintf("...%s", st[cutIndex:])
		} else {
			sm = fmt.Sprintf("...%s\n%s", st[cutIndex:], sm)
		}
		st = fmt.Sprintf("%s...", st[:cutIndex])
	}
	// adjust for line breaks
	newlineIndex := strings.Index(st, "\n")
	if newlineIndex > 0 {
		sm = fmt.Sprintf("%s\n%s", st[newlineIndex+1:], sm)
		st = st[:newlineIndex]

	}
	*sTitle = st
	*sMsg = sm
}

// WriteDataset writes a dataset to a cafs, replacing subcomponents of a dataset with path references
// during the write process. Directory structure is according to PackageFile naming conventions.
// This method is currently exported, but 99% of use cases should use CreateDataset instead of this
// lower-level function
func WriteDataset(store cafs.Filestore, ds *dataset.Dataset, pin bool) (string, error) {

	if ds == nil || ds.IsEmpty() {
		return "", fmt.Errorf("cannot save empty dataset")
	}
	name := ds.Name // preserve name for body file
	bodyFile := ds.BodyFile()
	fileTasks := 0
	addedDataset := false
	adder, err := store.NewAdder(pin, true)
	if err != nil {
		return "", fmt.Errorf("error creating new adder: %s", err.Error())
	}

	if ds.Viz != nil {
		ds.Viz.DropTransientValues()
		vizScript := ds.Viz.ScriptFile()
		vizRendered := ds.Viz.RenderedFile()
		// add task for the viz.json
		fileTasks++
		if vizRendered != nil {
			// add the rendered visualization
			// and add working group for adding the viz script file
			fileTasks += 2
			vrFile := qfs.NewMemfileReader(PackageFileRenderedViz.String(), vizRendered)
			defer vrFile.Close()
			adder.AddFile(vrFile)
		} else if vizScript != nil {
			// add the vizScript
			fileTasks++
			vsFile := qfs.NewMemfileReader(vizScriptFilename, vizScript)
			defer vsFile.Close()
			adder.AddFile(vsFile)
		} else {
			vizdata, err := json.Marshal(ds.Viz)
			if err != nil {
				return "", fmt.Errorf("error marshalling dataset viz to json: %s", err.Error())
			}
			adder.AddFile(qfs.NewMemfileBytes(PackageFileViz.String(), vizdata))
		}
	}

	if ds.Meta != nil {
		mdf, err := JSONFile(PackageFileMeta.String(), ds.Meta)
		if err != nil {
			return "", fmt.Errorf("error marshaling metadata to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(mdf)
	}

	if ds.Transform != nil {
		// TODO (b5): this is validation logic, should happen before WriteDataset is ever called
		// all resources must be references
		for key, r := range ds.Transform.Resources {
			if r.Path == "" {
				return "", fmt.Errorf("transform resource %s requires a path to save", key)
			}
		}

		sr := ds.Transform.ScriptFile()
		ds.Transform.DropTransientValues()
		if sr != nil {
			fileTasks++
			tsFile := qfs.NewMemfileReader(transformScriptFilename, sr)
			defer tsFile.Close()
			adder.AddFile(tsFile)
			// NOTE - add wg for the transform.json file ahead of time, which isn't completed
			// until after scriptPath has been added
			fileTasks++
		} else {
			tfdata, err := json.Marshal(ds.Transform)
			if err != nil {
				return "", fmt.Errorf("error marshalling dataset transform to json: %s", err.Error())
			}

			fileTasks++
			adder.AddFile(qfs.NewMemfileBytes(PackageFileTransform.String(), tfdata))
		}
	}

	if ds.Commit != nil {
		ds.Commit.DropTransientValues()
		cmf, err := JSONFile(PackageFileCommit.String(), ds.Commit)
		if err != nil {
			return "", fmt.Errorf("error marshilng dataset commit message to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(cmf)
	}

	if ds.Structure != nil {
		ds.Structure.DropTransientValues()
		stf, err := JSONFile(PackageFileStructure.String(), ds.Structure)
		if err != nil {
			return "", fmt.Errorf("error marshaling dataset structure to json: %s", err.Error())
		}
		fileTasks++
		adder.AddFile(stf)
	}

	fileTasks++
	adder.AddFile(bodyFile)

	var path string
	done := make(chan error, 0)
	go func() {
		for ao := range adder.Added() {
			path = ao.Path
			switch ao.Name {
			case PackageFileStructure.String():
				ds.Structure = dataset.NewStructureRef(ao.Path)
			case PackageFileTransform.String():
				ds.Transform = dataset.NewTransformRef(ao.Path)
			case PackageFileMeta.String():
				ds.Meta = dataset.NewMetaRef(ao.Path)
			case PackageFileCommit.String():
				ds.Commit = dataset.NewCommitRef(ao.Path)
			case PackageFileViz.String():
				ds.Viz = dataset.NewVizRef(ao.Path)
			case bodyFile.FileName():
				ds.BodyPath = ao.Path
				// ds.SetBodyFile(qfs.NewMemfileBytes(bodyFile.FileName(), bodyBytesBuf.Bytes()))
			case transformScriptFilename:
				ds.Transform.ScriptPath = ao.Path
				tfdata, err := json.Marshal(ds.Transform)
				if err != nil {
					done <- err
					return
				}
				// Add the encoded transform file, decrementing the stray fileTasks from above
				adder.AddFile(qfs.NewMemfileBytes(PackageFileTransform.String(), tfdata))
			case PackageFileRenderedViz.String():
				ds.Viz.RenderedPath = ao.Path
				vsFile := qfs.NewMemfileReader(vizScriptFilename, ds.Viz.ScriptFile())
				defer vsFile.Close()
				adder.AddFile(vsFile)
			case vizScriptFilename:
				ds.Viz.ScriptPath = ao.Path
				vizdata, err := json.Marshal(ds.Viz)
				if err != nil {
					done <- err
					return
				}
				// Add the encoded transform file, decrementing the stray fileTasks from above
				adder.AddFile(qfs.NewMemfileBytes(PackageFileViz.String(), vizdata))
			}

			fileTasks--
			if fileTasks == 0 {
				if !addedDataset {
					ds.DropTransientValues()
					dsdata, err := json.Marshal(ds)
					if err != nil {
						done <- err
						return
					}

					adder.AddFile(qfs.NewMemfileBytes(PackageFileDataset.String(), dsdata))
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
	if err != nil {
		return path, err
	}
	// TODO (b5): currently we're loading to keep the ds pointer hydrated post-write
	// we should remove that assumption, allowing callers to skip this load step, which may
	// be unnecessary
	var loaded *dataset.Dataset
	loaded, err = LoadDataset(store, path)
	if err != nil {
		return "", err
	}
	loaded.Name = name
	*ds = *loaded
	return path, nil
}
