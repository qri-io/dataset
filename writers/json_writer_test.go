package writers

import (
	"github.com/qri-io/dataset/datatypes"
	"testing"

	"github.com/qri-io/dataset"
)

func TestJsonWriter(t *testing.T) {

	cases := []struct {
		structure    *dataset.Structure
		writeObjects bool
		entries      [][][]byte
		out          string
	}{
		{&dataset.Structure{Schema: &dataset.Schema{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatypes.String}}}}, true, [][][]byte{[][]byte{[]byte("hello")}}, "[\n{\"a\":\"hello\"}\n]"},
		{&dataset.Structure{Schema: &dataset.Schema{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatypes.String}}}}, false, [][][]byte{[][]byte{[]byte("hello")}}, "[\n[\"hello\"]\n]"},
		{&dataset.Structure{Schema: &dataset.Schema{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatypes.String}}}}, true, [][][]byte{
			[][]byte{[]byte("hello")},
			[][]byte{[]byte("world")},
		}, "[\n{\"a\":\"hello\"},\n{\"a\":\"world\"}\n]"},
		// {&dataset.Structure{ Schema: &dataset.Schema{ Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatypes.String}}}}, false, [][][]byte{
		// 	[][]byte{[]byte("hello")},
		// 	[][]byte{[]byte("world")},
		// }, "[\n[\"hello\"],\n[\"world\"]\n]"},
		{&dataset.Structure{Schema: &dataset.Schema{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatypes.String}}}}, false, [][][]byte{
			[][]byte{[]byte("hello\n?")},
			[][]byte{[]byte("world")},
		}, "[\n[\"hello\\n?\"],\n[\"world\"]\n]"},
		{&dataset.Structure{Schema: &dataset.Schema{
			Fields: []*dataset.Field{
				&dataset.Field{Name: "ident", Type: datatypes.String},
				&dataset.Field{Name: "type", Type: datatypes.String},
				&dataset.Field{Name: "name", Type: datatypes.String},
				&dataset.Field{Name: "latitude_deg", Type: datatypes.Float},
				&dataset.Field{Name: "longitude_deg", Type: datatypes.Float},
				&dataset.Field{Name: "elevation_ft", Type: datatypes.Integer},
				&dataset.Field{Name: "continent", Type: datatypes.String},
				&dataset.Field{Name: "iso_country", Type: datatypes.String},
				&dataset.Field{Name: "iso_region", Type: datatypes.String},
				&dataset.Field{Name: "municipality", Type: datatypes.String},
				&dataset.Field{Name: "gps_code", Type: datatypes.String},
				&dataset.Field{Name: "iata_code", Type: datatypes.String},
				&dataset.Field{Name: "local_code", Type: datatypes.String},
				&dataset.Field{Name: "bool_teim", Type: datatypes.Boolean},
			}}},
			false,
			[][][]byte{
				[][]byte{[]byte("00AR"), []byte("heliport"), []byte("Newport Hospital & Clinic Heliport"), []byte{}, []byte{}, []byte{}, []byte("NA"), []byte("US"), []byte("US-AR"), []byte("Newport"), []byte("00AR"), []byte{}, []byte("00AR"), []byte{}},
			}, "[\n[\"00AR\",\"heliport\",\"Newport Hospital & Clinic Heliport\",0,0,0,\"NA\",\"US\",\"US-AR\",\"Newport\",\"00AR\",\"\",\"00AR\",false]\n]",
		},
	}

	for i, c := range cases {
		w := NewJsonWriter(c.structure, c.writeObjects)
		for _, ent := range c.entries {
			if err := w.WriteRow(ent); err != nil {
				t.Errorf("case %d WriteRow error: %s", i, err.Error())
				break
			}
		}
		if err := w.Close(); err != nil {
			t.Errorf("case %d Close error: %s", i, err.Error())
		}

		if string(w.Bytes()) != c.out {
			t.Errorf("case %d result mismatch. expected:\n%s\ngot:\n%s", i, c.out, string(w.Bytes()))
		}
	}
}
