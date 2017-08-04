package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestResouceHash(t *testing.T) {
	cases := []struct {
		r    *Resource
		hash string
		err  error
	}{
		{&Resource{Format: CsvDataFormat}, "12204ac9a6b596dd42656e7ea7ee3aadf755d92e769f94cc2c08af51aae80889e21b", nil},
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
		{"testdata/definitions/airport-codes.json", AirportCodes, nil},
		{"testdata/definitions/continent-codes.json", ContinentCodes, nil},
		{"testdata/definitions/hours.json", Hours, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Resource{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err = CompareResources(ds, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}

	}
}

func TestResourceMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Resource
		out []byte
		err error
	}{
		{&Resource{Format: CsvDataFormat}, []byte(`{"format":"csv","path":"","query":""}`), nil},
		{AirportCodes, []byte(`{"format":"csv","formatConfig":{"header_row":true},"path":"","query":"","schema":{"fields":[{"name":"ident","type":"string"},{"name":"type","type":"string"},{"name":"name","type":"string"},{"name":"latitude_deg","type":"float"},{"name":"longitude_deg","type":"float"},{"name":"elevation_ft","type":"integer"},{"name":"continent","type":"string"},{"name":"iso_country","type":"string"},{"name":"iso_region","type":"string"},{"name":"municipality","type":"string"},{"name":"gps_code","type":"string"},{"name":"iata_code","type":"string"},{"name":"local_code","type":"string"}]}}`), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if !bytes.Equal(c.out, got) {
			t.Errorf("case %d error mismatch. %s != %s", i, string(c.out), string(got))
			continue
		}
	}
}

func CompareResources(a, b *Resource) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("Resource mismatch: %s != %s", a, b)
	}

	return nil
}
