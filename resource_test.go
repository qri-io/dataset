package dataset

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestResourceUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Resource
		err      error
	}{
		{"airport-codes.json", AirportCodes, nil},
		{"continent-codes.json", ContinentCodes, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(filepath.Join("testdata/definitions", c.FileName))
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Resource{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d parse error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err = ResourceEqual(ds, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}

	}
}

func TestResourceMarshalJSON(t *testing.T) {

}

func ResourceEqual(a, b *Resource) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("Resource mismatch: %s != %s", a, b)
	}

	return nil
}
