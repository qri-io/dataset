package dstest

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestLoadTestCases(t *testing.T) {
	tcs, err := LoadTestCases("testdata")
	if err != nil {
		t.Error(err)
	}
	t.Logf("%d cases", len(tcs))
}

func TestNewTestCaseFromDir(t *testing.T) {

	if _, err := NewTestCaseFromDir("testdata"); err == nil {
		t.Errorf("expected error")
		return
	}

	tc, err := NewTestCaseFromDir("testdata/complete")
	if err != nil {
		t.Errorf("error reading test dir: %s", err.Error())
		return
	}

	name := "complete"
	if tc.Name != name {
		t.Errorf("expected name to equal: %s. got: %s", name, tc.Name)
	}

	fn := "data.csv"
	if tc.DataFilename != fn {
		t.Errorf("expected DataFilename to equal: %s. got: %s", fn, tc.DataFilename)
	}

	data := []byte(`city,pop,avg_age,in_usa
toronto,40000000,55.5,false
new york,8500000,44.4,true
chicago,300000,44.4,true
chatham,35000,65.25,true
raleigh,250000,50.65,true
`)
	if !bytes.Equal(tc.Data, data) {
		t.Errorf("data mismatch")
	}

	mf := tc.DataFile()
	if mf.FileName() != tc.DataFilename {
		t.Errorf("filename mismatch: %s != %s", mf.FileName(), tc.DataFilename)
	}

	mfdata, err := ioutil.ReadAll(mf)
	if err != nil {
		t.Errorf("error reading file: %s", err.Error())
	}

	if !bytes.Equal(mfdata, data) {
		t.Errorf("memfile data mismatch")
	}
}
