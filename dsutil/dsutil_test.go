package dsutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
	"github.com/qri-io/fs"
)

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

func testStore() (cafs.Filestore, map[string]string, error) {
	dataf := fs.NewMemfileBytes("movies.csv", []byte("movie\nup\nthe incredibles"))

	// Map strings to ds.keys for convenience
	ns := map[string]string{
		"movies": "",
	}

	ds := &dataset.Dataset{
		Structure: &dataset.Structure{
			Format: "csv",
			Schema: map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "array",
					"items": []interface{}{
						map[string]interface{}{"title": "movie", "type": "string"},
					},
				},
			},
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

func testStoreWithVizAndTransform() (cafs.Filestore, map[string]string, error) {
	ds := &dataset.Dataset{
		Structure: &dataset.Structure{
			Format: "csv",
			Schema: map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "array",
					"items": []interface{}{
						map[string]interface{}{"title": "movie", "type": "string"},
					},
				},
			},
		},
		Transform: &dataset.Transform{
			ScriptPath:  "transform_script",
			ScriptBytes: []byte("def transform(ds):\nreturn ds\n"),
		},
		Viz: &dataset.Viz{
			ScriptPath:  "viz_script",
			ScriptBytes: []byte("<html></html>\n"),
		},
	}
	// load scripts into file pointers, time for a NewDataset function?
	ds.Transform.ResolveScriptFile(nil)
	ds.Viz.ResolveScriptFile(nil)

	// Map strings to ds.keys for convenience
	ns := map[string]string{}
	// Store the files
	st := cafs.NewMapstore()
	dataf := fs.NewMemfileBytes("movies.csv", []byte("movie\nup\nthe incredibles"))
	dskey, err := dsfs.WriteDataset(st, ds, dataf, true)
	if err != nil {
		return st, ns, err
	}
	ns["movies"] = dskey
	ns["transform_script"] = ds.Transform.ScriptPath
	ns["viz_template"] = ds.Viz.ScriptPath
	return st, ns, nil
}

func testdataFile(base string) string {
	return filepath.Join(os.Getenv("GOPATH"), "/src/github.com/qri-io/dataset/testdata/"+base)
}
