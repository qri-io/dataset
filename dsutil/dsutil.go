// Package dsutil includes dataset util funcs, placed here to avoid dataset package bloat
// TODO - consider merging this package with the dsfs package, as most of the functions in
// here rely on a Filestore argument
package dsutil

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ipfs/go-datastore"
	logger "github.com/ipfs/go-log"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
)

var log = logger.Logger("dsutil")

// WriteZipArchive generates a zip archive of a dataset and writes it to w
func WriteZipArchive(store cafs.Filestore, ds *dataset.Dataset, ref string, w io.Writer) error {
	zw := zip.NewWriter(w)

	// Dataset header, contains meta, structure, and commit
	dsf, err := zw.Create(dsfs.PackageFileDataset.String())
	if err != nil {
		log.Debug(err.Error())
		return err
	}
	dsdata, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		return err
	}
	_, err = dsf.Write(dsdata)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	// Reference to dataset, as a string
	target, err := zw.Create("ref.txt")
	if err != nil {
		return err
	}
	_, err = io.WriteString(target, ref)
	if err != nil {
		return err
	}

	// Transform script
	if ds.Transform != nil && ds.Transform.ScriptPath != "" {
		script, err := store.Get(datastore.NewKey(ds.Transform.ScriptPath))
		if err != nil {
			return err
		}
		target, err := zw.Create("transform.sky")
		if err != nil {
			return err
		}
		_, err = io.Copy(target, script)
		if err != nil {
			return err
		}
	}

	// Viz template
	if ds.Viz != nil && ds.Viz.ScriptPath != "" {
		script, err := store.Get(datastore.NewKey(ds.Viz.ScriptPath))
		if err != nil {
			return err
		}
		target, err := zw.Create("viz.html")
		if err != nil {
			return err
		}
		_, err = io.Copy(target, script)
		if err != nil {
			return err
		}
	}

	// Body
	datadst, err := zw.Create(fmt.Sprintf("body.%s", ds.Structure.Format.String()))
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	datasrc, err := dsfs.LoadBody(store, ds)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	if _, err = io.Copy(datadst, datasrc); err != nil {
		log.Debug(err.Error())
		return err
	}

	return zw.Close()
}

// WriteDir loads a dataset & writes all contents to a directory specified by path
func WriteDir(store cafs.Filestore, ds *dataset.Dataset, path string) error {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		log.Debug(err.Error())
		return err
	}

	dsdata, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		log.Debug(err.Error())
		return err
	}
	err = ioutil.WriteFile(filepath.Join(path, dsfs.PackageFileDataset.String()), dsdata, os.ModePerm)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	datasrc, err := dsfs.LoadBody(store, ds)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	datadst, err := os.Create(filepath.Join(path, fmt.Sprintf("data.%s", ds.Structure.Format.String())))
	if err != nil {
		log.Debug(err.Error())
		return err
	}
	if _, err = io.Copy(datadst, datasrc); err != nil {
		log.Debug(err.Error())
		return err
	}

	return nil
}
