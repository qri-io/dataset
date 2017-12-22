package dsutil

import (
	"archive/zip"
	"bytes"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset/datatypes"
	"github.com/qri-io/dataset/dsfs"
	"os"
	"path/filepath"
	"testing"

	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

func TestWriteZipArchive(t *testing.T) {
	store, names, err := testStore()
	if err != nil {
		t.Errorf("error creating store: %s", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(store, names["movies"])
	if err != nil {
		t.Errorf("error fetching movies dataset from store: %s", err.Error())
		return
	}

	buf := &bytes.Buffer{}
	if err = WriteZipArchive(store, ds, buf); err != nil {
		t.Errorf("error writing zip archive: %s", err.Error())
		return
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Errorf("error creating zip reader: %s", err.Error())
		return
	}

	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Errorf("error opening file %s in package", f.Name)
			break
		}

		if err := rc.Close(); err != nil {
			t.Errorf("error closing file %s in package", f.Name)
			break
		}
	}
}

func TestWriteDir(t *testing.T) {
	store, names, err := testStore()
	if err != nil {
		t.Errorf("error creating store: %s", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(store, names["movies"])
	if err != nil {
		t.Errorf("error fetching movies dataset from store: %s", err.Error())
		return
	}

	dir := filepath.Join(os.TempDir(), "dsutil_test_write_dir")
	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		t.Errorf("error creating temp directory: %s", err.Error())
		return
	}

	if err = WriteDir(store, ds, dir); err != nil {
		t.Errorf("error writing directory: %s", err.Error())
		return
	}

	// TODO - check files in directory are clean

	if err = os.RemoveAll(dir); err != nil {
		t.Errorf("error cleaning up after writeDir test: %s", err.Error())
		return
	}
}

func testStore() (cafs.Filestore, map[string]datastore.Key, error) {
	fs := memfs.NewMapstore()
	ns := map[string]datastore.Key{
		"movies": datastore.NewKey(""),
	}

	dataf := memfs.NewMemfileBytes("movies.csv", []byte("movie\nup\nthe incredibles"))

	ds := &dataset.Dataset{
		Structure: &dataset.Structure{
			Format: dataset.CSVDataFormat,
			Schema: &dataset.Schema{
				Fields: []*dataset.Field{
					{Name: "movie", Type: datatypes.String},
				},
			},
		},
	}

	dskey, err := dsfs.WriteDataset(fs, ds, dataf, true)
	if err != nil {
		return fs, ns, err
	}
	ns["movies"] = dskey

	return fs, ns, nil
}
