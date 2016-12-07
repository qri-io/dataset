// Dataset util funcs, placed here to avoid dataset package bloat
package dsutil

import (
	"archive/zip"
	"bytes"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/fs"
)

// PackageDataset creates a zip archive from a store & address
func PackageDataset(store fs.Store, ds *dataset.Dataset) (io.ReaderAt, int64, error) {
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)

	if err := writeDatasetFiles(zw, store, ds); err != nil {
		return nil, 0, err
	}

	for _, d := range ds.Datasets {
		writeDatasetFiles(zw, store, d)
	}

	if err := zw.Close(); err != nil {
		return nil, 0, err
	}

	return bytes.NewReader(buf.Bytes()), int64(len(buf.Bytes())), nil
}

func writeDatasetFiles(zw *zip.Writer, store fs.Store, ds *dataset.Dataset) error {
	// Grab dataset file if one is listed
	if ds.File != "" {
		data, err := store.Read(ds.File)
		if err != nil {
			return err
		}

		w, err := zw.Create(ds.File)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}

	return nil
}
