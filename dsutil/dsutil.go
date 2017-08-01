// Dataset util funcs, placed here to avoid dataset package bloat
package dsutil

// import (
// 	"archive/zip"
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"io/ioutil"
// 	"path/filepath"

// 	"github.com/ipfs/go-datastore"
// 	"github.com/qri-io/dataset"
// 	"github.com/qri-io/fs"
// )

// // PackageDataset creates a zip archive from a store & address
// func PackageDataset(store datastore.Datastore, r *dataset.Dataset) (io.ReaderAt, int64, error) {
// 	buf := &bytes.Buffer{}
// 	zw := zip.NewWriter(buf)

// 	w, err := zw.Create(dataset.Filename)
// 	if err != nil {
// 		return nil, 0, err
// 	}
// 	data, err := json.Marshal(r)
// 	if err != nil {
// 		return nil, 0, err
// 	}
// 	if _, err := w.Write(data); err != nil {
// 		return nil, 0, err
// 	}

// 	if err := writeDatasetFiles(zw, store, ds); err != nil {
// 		return nil, 0, err
// 	}

// 	for _, d := range ds.Datasets {
// 		writeDatasetFiles(zw, store, d)
// 	}

// 	if err := zw.Close(); err != nil {
// 		return nil, 0, err
// 	}

// 	return bytes.NewReader(buf.Bytes()), int64(len(buf.Bytes())), nil
// }

// func writeDatasetFiles(zw *zip.Writer, store fs.Store, ds *dataset.Dataset) error {
// 	// Grab dataset file if one is listed
// 	if ds.File != "" {
// 		if err := zipWriteFile(ds.File, zw, store, ds); err != nil {
// 			return err
// 		}
// 	}

// 	if ds.Readme != "" {
// 		if err := zipWriteFile(ds.Readme, zw, store, ds); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// func zipWriteFile(path string, zw *zip.Writer, store fs.Store, ds *dataset.Dataset) error {
// 	data, err := store.Read(fs.JoinPath(ds.Address.PathString(), path))
// 	// data, err := store.Read(path)
// 	if err != nil {
// 		return err
// 	}

// 	w, err := zw.Create(path)
// 	if err != nil {
// 		return err
// 	}
// 	if _, err := w.Write(data); err != nil {
// 		return err
// 	}

// 	return nil
// }

// func WritePackage(store fs.Store, adr dataset.Address, r io.ReaderAt, size int64) error {
// 	zipr, err := zip.NewReader(r, size)
// 	if err != nil {
// 		return err
// 	}

// 	for _, f := range zipr.File {
// 		r, err := f.Open()
// 		if err != nil {
// 			return err
// 		}

// 		data, err := ioutil.ReadAll(r)
// 		if err != nil {
// 			return err
// 		}

// 		if err := store.Write(filepath.Join(adr.PathString(), f.Name), data); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// func PackageDatasetDefinition(r io.ReaderAt, size int64) (*dataset.Dataset, error) {
// 	zipr, err := zip.NewReader(r, size)
// 	if err != nil {
// 		return nil, err
// 	}

// 	for _, f := range zipr.File {
// 		if f.Name == dataset.Filename {
// 			r, err := f.Open()
// 			if err != nil {
// 				return nil, err
// 			}

// 			data, err := ioutil.ReadAll(r)
// 			if err != nil {
// 				return nil, err
// 			}

// 			ds := &dataset.Dataset{}
// 			err = json.Unmarshal(data, ds)
// 			return ds, err
// 		}
// 	}

// 	return nil, fmt.Errorf("no %s file found", dataset.Filename)
// }
