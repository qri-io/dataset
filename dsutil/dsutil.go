// Dataset util funcs, placed here to avoid dataset package bloat
package dsutil

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

func WriteZipArchive(store cafs.Filestore, ds *dataset.Dataset, w io.Writer) error {
	zw := zip.NewWriter(w)

	dsf, err := zw.Create(dsfs.PackageFileDataset.String())
	if err != nil {
		return err
	}
	dsdata, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		return err
	}
	_, err = dsf.Write(dsdata)
	if err != nil {
		return err
	}

	datadst, err := zw.Create(fmt.Sprintf("data.%s", ds.Structure.Format.String()))
	if err != nil {
		return err
	}

	datasrc, err := dsfs.LoadDatasetData(store, ds)
	if err != nil {
		return err
	}

	if _, err = io.Copy(datadst, datasrc); err != nil {
		return err
	}

	return zw.Close()
}

func WriteDir(store cafs.Filestore, ds *dataset.Dataset, path string) error {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return err
	}

	dsdata, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filepath.Join(path, dsfs.PackageFileDataset.String()), dsdata, os.ModePerm)
	if err != nil {
		return err
	}

	datasrc, err := dsfs.LoadDatasetData(store, ds)
	if err != nil {
		return err
	}

	datadst, err := os.Create(filepath.Join(path, fmt.Sprintf("data.%s", ds.Structure.Format.String())))
	if err != nil {
		return err
	}
	if _, err = io.Copy(datadst, datasrc); err != nil {
		return err
	}

	return nil
}

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
