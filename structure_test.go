package dataset

import (
	"bytes"
	"encoding/json"
	"github.com/qri-io/dataset/compression"
	"github.com/qri-io/jsonschema"
	"io/ioutil"
	"testing"

	"github.com/ipfs/go-datastore"
	// "github.com/qri-io/dataset/datatypes"
)

func TestStrucureHash(t *testing.T) {
	cases := []struct {
		r    *Structure
		hash string
		err  error
	}{
		{&Structure{Kind: KindStructure, Format: CSVDataFormat}, "QmfJRjmdxpZKrWvJeVzFwrB5UTK45xs9FB4Uv7EJYfNwyW", nil},
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

func TestAbstractColumnName(t *testing.T) {
	if AbstractColumnName(0) != "a" {
		t.Errorf("expected 0 == a")
	}
	// I found the h button & pushed it twice.
	if AbstractColumnName(215) != "hh" {
		t.Errorf("expected 26 == hh, got: %s", AbstractColumnName(215))
	}
	if AbstractColumnName(30000) != "ariw" {
		t.Errorf("expected 300 == ariw, got: %s", AbstractColumnName(30000))
	}
}

func TestStructureAbstract(t *testing.T) {
	cases := []struct {
		in, out *Structure
	}{
		{AirportCodesStructure, AirportCodesStructureAbstract},
	}

	for i, c := range cases {
		if err := CompareStructures(c.in.Abstract(), c.out); err != nil {
			t.Errorf("case %d error: %s", i, err.Error())
			continue
		}
	}
}

func TestStructureIsEmpty(t *testing.T) {
	cases := []struct {
		st *Structure
	}{
		{&Structure{Checksum: "a"}},
		{&Structure{Compression: compression.Tar}},
		{&Structure{Encoding: "a"}},
		{&Structure{Entries: 1}},
		{&Structure{Format: CSVDataFormat}},
		{&Structure{FormatConfig: &CSVOptions{}}},
		{&Structure{Length: 1}},
		{&Structure{Schema: &jsonschema.RootSchema{}}},
	}

	for i, c := range cases {
		if c.st.IsEmpty() == true {
			t.Errorf("case %d improperly reported dataset as empty", i)
			continue
		}
	}
}

func TestStructureAssign(t *testing.T) {
	expect := &Structure{
		Format: CSVDataFormat,
		Length: 2503,
		// TODO - restore
		// Schema: &Schema{
		// 	Fields: []*Field{
		// 		{Type: datatypes.String, Name: "foo"},
		// 		{Type: datatypes.Integer, Name: "bar"},
		// 		{Description: "bat"},
		// 	},
		// },
	}
	got := &Structure{
		Format: CSVDataFormat,
		// Schema: &Schema{
		// 	Fields: []*Field{
		// 		{Type: datatypes.String},
		// 		{Type: datatypes.Integer},
		// 	},
		// },
	}

	got.Assign(&Structure{
		Length: 2503,
		// Schema: &Schema{
		// 	Fields: []*Field{
		// 		{Name: "foo"},
		// 		{Name: "bar"},
		// 		{Description: "bat"},
		// 	},
		// },
	})

	if err := CompareStructures(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareStructures(expect, got); err != nil {
		t.Error(err)
	}

	emptySt := &Structure{}
	emptySt.Assign(expect)
	if err := CompareStructures(expect, emptySt); err != nil {
		t.Error(err)
	}
}

func TestStructureUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Structure
		err      error
	}{
		{"testdata/structures/airport-codes.json", AirportCodesStructure, nil},
		{"testdata/structures/continent-codes.json", ContinentCodesStructure, nil},
		{"testdata/structures/hours.json", HoursStructure, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(c.FileName)
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		st := &Structure{}
		if err := json.Unmarshal(data, st); err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err = CompareStructures(st, c.result); err != nil {
			t.Errorf("case %d resource comparison error: %s", i, err)
			continue
		}
	}

	strq := &Structure{}
	path := "/path/to/structure"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strq); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strq.path.String() != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strq.path)
		return
	}
}

func TestStructureMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Structure
		out []byte
		err error
	}{
		{&Structure{Format: CSVDataFormat}, []byte(`{"format":"csv","kind":"qri:st:0"}`), nil},
		{&Structure{Format: CSVDataFormat, Kind: KindStructure}, []byte(`{"format":"csv","kind":"qri:st:0"}`), nil},
		{AirportCodesStructure, []byte(`{"format":"csv","formatConfig":{"headerRow":true},"kind":"qri:st:0","schema":{"items":{"items":[{"title":"ident","type":"string"},{"title":"type","type":"string"},{"title":"name","type":"string"},{"title":"latitude_deg","type":"string"},{"title":"longitude_deg","type":"string"},{"title":"elevation_ft","type":"string"},{"title":"continent","type":"string"},{"title":"iso_country","type":"string"},{"title":"iso_region","type":"string"},{"title":"municipality","type":"string"},{"title":"gps_code","type":"string"},{"title":"iata_code","type":"string"},{"title":"local_code","type":"string"}],"type":"array"},"type":"array"}}`), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if string(c.out) != string(got) {
			t.Errorf("case %d error mismatch. %s != %s", i, string(c.out), string(got))
			continue
		}
	}

	strbytes, err := json.Marshal(&Structure{path: datastore.NewKey("/path/to/structure")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/structure\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/structure\"")
	}
}

func TestUnmarshalStructure(t *testing.T) {
	sta := Structure{Kind: KindStructure, Format: CSVDataFormat}
	cases := []struct {
		value interface{}
		out   *Structure
		err   string
	}{
		{sta, &sta, ""},
		{&sta, &sta, ""},
		{[]byte("{\"kind\":\"qri:st:0\"}"), &Structure{Kind: KindStructure}, ""},
		{5, nil, "couldn't parse structure, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalStructure(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareStructures(c.out, got); err != nil {
			t.Errorf("case %d structure mismatch: %s", i, err.Error())
			continue
		}
	}
}
