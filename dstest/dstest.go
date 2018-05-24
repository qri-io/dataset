// Package dstest defines an interface for reading test cases from static files
// leveraging directories of test dataset input files & expected output files
package dstest

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

var log = logger.Logger("dstest")

const (
	// InputDatasetFilename is the filename to use for an input dataset
	InputDatasetFilename = "input.dataset.json"
	// ExpectDatasetFilename is the filename to use to compare expected outputs
	ExpectDatasetFilename = "expect.dataset.json"
)

// TestCase is a dataset test case, usually built from a
// directory of files for use in tests.
// All files are optional for TestCase, but may be required
// by the test itself.
type TestCase struct {
	// Path to the director on the local filesystem this test case is loaded from
	Path string
	// Name is the casename, should match directory name
	Name string
	// 	 data.csv,data.json, etc
	DataFilename string
	// test data in expected data format
	Data []byte
	// Filename of Transform Script
	TransformScriptFilename string
	// TransformScript bytes if one exists
	TransformScript []byte
	// Input is intended file for test input
	// loads from input.dataset.json
	Input *dataset.Dataset
	//  Expect should match test output
	// loads from expect.dataset.json
	Expect *dataset.Dataset
}

// DataFile creates a new in-memory file from data & filename properties
func (t TestCase) DataFile() cafs.File {
	return cafs.NewMemfileBytes(t.DataFilename, t.Data)
}

// TransformScriptFile creates a cafs.File from testCase tranform script data
func (t TestCase) TransformScriptFile() (cafs.File, bool) {
	if t.TransformScript == nil {
		return nil, false
	}
	return cafs.NewMemfileBytes(t.TransformScriptFilename, t.TransformScript), true
}

// DataFilepath retuns the path to the first valid data file it can find,
// which is a file named "data" that ends in an extension we support
func DataFilepath(dir string) (string, error) {
	for _, df := range dataset.SupportedDataFormats() {
		path := fmt.Sprintf("%s/data.%s", dir, df)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return path, nil
		}
	}
	return "", os.ErrNotExist
}

// LoadTestCases loads a directory of case directories
func LoadTestCases(dir string) (tcs map[string]TestCase, err error) {
	tcs = map[string]TestCase{}
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}
	for _, fi := range fis {
		if fi.IsDir() {
			if tc, err := NewTestCaseFromDir(filepath.Join(dir, fi.Name())); err == nil {
				tcs[fi.Name()] = tc
			}
		}
	}
	return
}

// NewTestCaseFromDir creates a test case from a directory of static test files
// dir should be the path to the directory to check, and any parsing errors will
// be logged using t.Log methods
func NewTestCaseFromDir(dir string) (tc TestCase, err error) {
	tc = TestCase{
		Path: dir,
		Name: filepath.Base(dir),
	}
	tc.Data, tc.DataFilename, err = ReadInputData(dir)
	if err != nil {
		err = fmt.Errorf("error reading test case data for directory %s: %s", dir, err.Error())
		log.Info(err.Error())
		return
	}

	if tc.TransformScript, tc.TransformScriptFilename, err = ReadInputTransformScript(dir); err != nil {
		if err == os.ErrNotExist {
			// TransformScript is optional, so if this errors, let's bail
			err = nil
		} else {
			return tc, fmt.Errorf("reading transform script: %s", err.Error())
		}
	}

	tc.Input, err = ReadDataset(dir, InputDatasetFilename)
	if err != nil && !os.IsNotExist(err) {
		msg := fmt.Sprintf("%s: error loading input dataset: %s", tc.Name, err)
		log.Infof(msg)
	}
	err = nil

	tc.Expect, err = ReadDataset(dir, ExpectDatasetFilename)
	if err != nil && !os.IsNotExist(err) {
		msg := fmt.Sprintf("%s: error loading expect dataset: %s", tc.Name, err)
		log.Info(msg)
	}
	err = nil

	return
}

// ReadDataset grabs a dataset for a given dir for a given filename
func ReadDataset(dir, filename string) (*dataset.Dataset, error) {
	data, err := ioutil.ReadFile(filepath.Join(dir, filename))
	if err != nil {
		log.Info(err.Error())
		return nil, err
	}

	ds := &dataset.Dataset{}
	return ds, ds.UnmarshalJSON(data)
}

// ReadInputData grabs input data
func ReadInputData(dir string) ([]byte, string, error) {
	for _, df := range dataset.SupportedDataFormats() {
		path := fmt.Sprintf("%s/data.%s", dir, df)
		if f, err := os.Open(path); err == nil {
			data, err := ioutil.ReadAll(f)
			return data, fmt.Sprintf("data.%s", df), err
		}
	}
	return nil, "", os.ErrNotExist
}

// ReadInputTransformScript grabs input transform bytes
func ReadInputTransformScript(dir string) ([]byte, string, error) {
	path := filepath.Join(dir, "transform.sky")
	if f, err := os.Open(path); err == nil {
		data, err := ioutil.ReadAll(f)
		return data, "transform.sky", err
	}
	return nil, "", os.ErrNotExist
}
