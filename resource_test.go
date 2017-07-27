package dataset

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestResouceHash(t *testing.T) {
	cases := []struct {
		r    *Resource
		hash string
		err  error
	}{
		{&Resource{Format: CsvDataFormat}, "1220c2f881931bffda4b33de1fcc9c6085b4d4b9dcc5d18083d97c6415c1a3590b66", nil},
	}

	for i, c := range cases {
		hash, err := c.r.Hash()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected %s, got %s", i, c.err, err)
			continue
		}

		if hash != c.hash {
			t.Errorf("case %d hash mismatch. expected %s, got %s", i, c.hash, hash)
			continue
		}
	}
}

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

		if err = CompareResources(ds, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}

	}
}

func TestResourceMarshalJSON(t *testing.T) {

}

func CompareResources(a, b *Resource) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("Resource mismatch: %s != %s", a, b)
	}

	return nil
}
