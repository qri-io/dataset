package dataset

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/qri-io/dataset/compression"
)

func TestStrucureHash(t *testing.T) {
	cases := []struct {
		r    *Structure
		hash string
		err  error
	}{
		{&Structure{Qri: KindStructure.String(), Format: "csv"}, "QmUqNTfVuJamhRfXLC1QUZ8RLaGhUaTY31ChX4GbtamW2o", nil},
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

func TestStructureJSONSchema(t *testing.T) {
	t.Skip("TODO (b5)")
}

func TestStructureDataFormat(t *testing.T) {
	t.Skip("TODO (b5)")
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
		{&Structure{Compression: compression.Tar.String()}},
		{&Structure{Depth: 1}},
		{&Structure{Encoding: "a"}},
		{&Structure{Entries: 1}},
		{&Structure{ErrCount: 1}},
		{&Structure{Format: "csv"}},
		{&Structure{FormatConfig: map[string]interface{}{}}},
		{&Structure{Length: 1}},
		{&Structure{Schema: map[string]interface{}{}}},
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
		Length:      2503,
		Checksum:    "hey",
		Compression: compression.Gzip.String(),
		Depth:       11,
		ErrCount:    12,
		Encoding:    "UTF-8",
		Entries:     3000000000,
		Format:      "csv",
	}
	got := &Structure{
		Length: 2000,
		Format: "json",
	}

	got.Assign(&Structure{
		Length:      2503,
		Checksum:    "hey",
		Compression: compression.Gzip.String(),
		Depth:       11,
		ErrCount:    12,
		Encoding:    "UTF-8",
		Entries:     3000000000,
		Format:      "csv",
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

	if strq.Path != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strq.Path)
		return
	}
}

func TestStructureMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *Structure
		out []byte
		err error
	}{
		{&Structure{Format: "csv"}, []byte(`{"errCount":0,"format":"csv","qri":"st:0"}`), nil},
		{&Structure{Format: "csv", Qri: KindStructure.String()}, []byte(`{"errCount":0,"format":"csv","qri":"st:0"}`), nil},
		{AirportCodesStructure, []byte(`{"errCount":5,"format":"csv","formatConfig":{"headerRow":true},"qri":"st:0","schema":{"items":{"items":[{"title":"ident","type":"string"},{"title":"type","type":"string"},{"title":"name","type":"string"},{"title":"latitude_deg","type":"number"},{"title":"longitude_deg","type":"number"},{"title":"elevation_ft","type":"integer"},{"title":"continent","type":"string"},{"title":"iso_country","type":"string"},{"title":"iso_region","type":"string"},{"title":"municipality","type":"string"},{"title":"gps_code","type":"string"},{"title":"iata_code","type":"string"},{"title":"local_code","type":"string"}],"type":"array"},"type":"array"}}`), nil},
		{&Structure{Path: "/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"}, []byte(`"/map/QmUaMozKVkjPf7CVf3Zd8Cy5Ex1i9oUdhYhU8uTJph5iFD"`), nil},
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

	strbytes, err := json.Marshal(&Structure{Path: "/path/to/structure"})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/structure\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/structure\"")
	}
}

func TestStructureMarshalJSONObject(t *testing.T) {
	cases := []struct {
		in  *Structure
		out []byte
		err error
	}{
		{&Structure{Format: "csv"}, []byte(`{"errCount":0,"format":"csv","qri":"st:0"}`), nil},
		{&Structure{Format: "csv", Qri: KindStructure.String()}, []byte(`{"errCount":0,"format":"csv","qri":"st:0"}`), nil},
		{AirportCodesStructure, []byte(`{"errCount":5,"format":"csv","formatConfig":{"headerRow":true},"qri":"st:0","schema":{"items":{"items":[{"title":"ident","type":"string"},{"title":"type","type":"string"},{"title":"name","type":"string"},{"title":"latitude_deg","type":"string"},{"title":"longitude_deg","type":"string"},{"title":"elevation_ft","type":"string"},{"title":"continent","type":"string"},{"title":"iso_country","type":"string"},{"title":"iso_region","type":"string"},{"title":"municipality","type":"string"},{"title":"gps_code","type":"string"},{"title":"iata_code","type":"string"},{"title":"local_code","type":"string"}],"type":"array"},"type":"array"}}`), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSONObject()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		// now try to unmarshal to map string interface
		check := &map[string]interface{}{}
		err = json.Unmarshal(got, check)
		if err != nil {
			t.Errorf("case %d error: failed to unmarshal to object: %s", i, err.Error())
			continue
		}
	}
}

func TestUnmarshalStructure(t *testing.T) {
	sta := Structure{Qri: KindStructure.String(), Format: "csv"}
	cases := []struct {
		value interface{}
		out   *Structure
		err   string
	}{
		{sta, &sta, ""},
		{&sta, &sta, ""},
		{[]byte("{\"qri\":\"st:0\"}"), &Structure{Qri: KindStructure.String()}, ""},
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
