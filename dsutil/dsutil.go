// Package dsutil includes dataset util funcs, placed here to avoid dataset package bloat
// TODO - consider merging this package with the dsfs package, as most of the functions in
// here rely on a Filestore argument
package dsutil

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
)

var log = logger.Logger("dsutil")

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
