package dsutil

import (
	"archive/zip"
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
	"github.com/qri-io/jsonschema"
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
	if err = WriteZipArchive(store, ds, "testref", buf); err != nil {
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

func TestWriteZipArchiveFullDataset(t *testing.T) {
	store, names, err := testStoreWithVizAndTransform()
	if err != nil {
		t.Errorf("error creating store: %s", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(store, names["movies"])
	if err != nil {
		t.Errorf("error fetching movies dataset from store: %s", err.Error())
		return
	}

	_, err = store.Get(names["transform_script"])
	if err != nil {
		t.Errorf("error fetching movies dataset from store: %s", err.Error())
		return
	}

	buf := &bytes.Buffer{}
	if err = WriteZipArchive(store, ds, "testref", buf); err != nil {
		t.Errorf("error writing zip archive: %s", err.Error())
		return
	}

	tmppath := filepath.Join(os.TempDir(), "exported.zip")
	defer os.RemoveAll(tmppath)

	err = ioutil.WriteFile(tmppath, buf.Bytes(), os.ModePerm)
	if err != nil {
		t.Errorf("error writing temp zip file: %s", err.Error())
		return
	}

	expectFile := testdataFile("zip/exported.zip")
	expectBytes, err := ioutil.ReadFile(expectFile)
	if err != nil {
		t.Errorf("error reading expected bytes: %s", err.Error())
		return
	}
	if !bytes.Equal(buf.Bytes(), expectBytes) {
		t.Errorf("error bytes of exported zip did not match")
		return
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
	dataf := cafs.NewMemfileBytes("movies.csv", []byte("movie\nup\nthe incredibles"))

	// Map strings to ds.keys for convenience
	ns := map[string]datastore.Key{
		"movies": datastore.NewKey(""),
	}

	ds := &dataset.Dataset{
		Structure: &dataset.Structure{
			Format: dataset.CSVDataFormat,
			Schema: jsonschema.Must(`{
				"type": "array",
				"items": {
					"type":"array",
					"items" : [
						{"title": "movie", "type": "string"}
					]
				}
			}`),
		},
	}

	fs := cafs.NewMapstore()
	dskey, err := dsfs.WriteDataset(fs, ds, dataf, true)
	if err != nil {
		return fs, ns, err
	}
	ns["movies"] = dskey

	return fs, ns, nil
}

func testStoreWithVizAndTransform() (cafs.Filestore, map[string]datastore.Key, error) {
	ds := &dataset.Dataset{
		Structure: &dataset.Structure{
			Format: dataset.CSVDataFormat,
			Schema: jsonschema.Must(`{
				"type": "array",
				"items": {
					"type":"array",
					"items" : [
						{"title": "movie", "type": "string"}
					]
				}
			}`),
		},
		Transform: &dataset.Transform{
			ScriptPath: "transform_script",
			Script:     strings.NewReader("def transform(ds):\nreturn ds\n"),
		},
		Viz: &dataset.Viz{
			ScriptPath: "viz_script",
			Script:     strings.NewReader("<html></html>\n"),
		},
	}
	// Map strings to ds.keys for convenience
	ns := map[string]datastore.Key{}
	// Store the files
	fs := cafs.NewMapstore()
	dataf := cafs.NewMemfileBytes("movies.csv", []byte("movie\nup\nthe incredibles"))
	dskey, err := dsfs.WriteDataset(fs, ds, dataf, true)
	if err != nil {
		return fs, ns, err
	}
	ns["movies"] = dskey
	ns["transform_script"] = datastore.NewKey(ds.Transform.ScriptPath)
	ns["viz_template"] = datastore.NewKey(ds.Viz.ScriptPath)
	return fs, ns, nil
}

func testdataFile(base string) string {
	return filepath.Join(os.Getenv("GOPATH"), "/src/github.com/qri-io/dataset/testdata/"+base)
}
