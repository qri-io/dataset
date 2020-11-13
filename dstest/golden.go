package dstest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/qri-io/dataset"
)

// UpdateGoldenFileEnvVarname is the envionment variable that dstest checks
// before writing
const UpdateGoldenFileEnvVarname = "QRI_UPDATE_GOLDEN_FILES"

// LoadGoldenFile loads a dataset from a JSON file
func LoadGoldenFile(t *testing.T, filename string) *dataset.Dataset {
	t.Helper()
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("opening JSON golden file: %s", err)
	}

	ds := &dataset.Dataset{}
	if err := json.Unmarshal(data, ds); err != nil {
		t.Fatalf("unmarshaling JSON golden file: %s", err)
	}

	return ds
}

// UpdateGoldenFileIfEnvVarSet overwrites the given filename if
// QRI_UPDATED_GOLDEN_FILES env var is set
func UpdateGoldenFileIfEnvVarSet(filename string, got *dataset.Dataset) {
	if os.Getenv(UpdateGoldenFileEnvVarname) != "" {
		fmt.Printf("updating golden file: %q\n", filename)
		data, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			panic(err)
		}
		if err := ioutil.WriteFile(filename, data, 0644); err != nil {
			panic(err)
		}
	}
}
