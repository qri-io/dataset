package writers

// import (
// 	"testing"

// 	"github.com/qri-io/dataset"
// 	"github.com/qri-io/datatype"
// )

// func TestJsonWriter(t *testing.T) {

// 	cases := []struct {
// 		ds           *dataset.Dataset
// 		writeObjects bool
// 		entries      [][][]byte
// 		out          string
// 	}{
// 		{&dataset.Dataset{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatype.String}}}, true, [][][]byte{[][]byte{[]byte("hello")}}, "[\n{\"a\":\"hello\"}\n]"},
// 		{&dataset.Dataset{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatype.String}}}, false, [][][]byte{[][]byte{[]byte("hello")}}, "[\n[\"hello\"]\n]"},
// 		{&dataset.Dataset{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatype.String}}}, true, [][][]byte{
// 			[][]byte{[]byte("hello")},
// 			[][]byte{[]byte("world")},
// 		}, "[\n{\"a\":\"hello\"},\n{\"a\":\"world\"}\n]"},
// 		{&dataset.Dataset{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatype.String}}}, false, [][][]byte{
// 			[][]byte{[]byte("hello")},
// 			[][]byte{[]byte("world")},
// 		}, "[\n[\"hello\"],\n[\"world\"]\n]"},
// 		{&dataset.Dataset{Fields: []*dataset.Field{&dataset.Field{Name: "a", Type: datatype.String}}}, false, [][][]byte{
// 			[][]byte{[]byte("hello\n?")},
// 			[][]byte{[]byte("world")},
// 		}, "[\n[\"hello\\n?\"],\n[\"world\"]\n]"},
// 		{&dataset.Dataset{Fields: []*dataset.Field{
// 			&dataset.Field{Name: "ident", Type: datatype.String},
// 			&dataset.Field{Name: "type", Type: datatype.String},
// 			&dataset.Field{Name: "name", Type: datatype.String},
// 			&dataset.Field{Name: "latitude_deg", Type: datatype.Float},
// 			&dataset.Field{Name: "longitude_deg", Type: datatype.Float},
// 			&dataset.Field{Name: "elevation_ft", Type: datatype.Integer},
// 			&dataset.Field{Name: "continent", Type: datatype.String},
// 			&dataset.Field{Name: "iso_country", Type: datatype.String},
// 			&dataset.Field{Name: "iso_region", Type: datatype.String},
// 			&dataset.Field{Name: "municipality", Type: datatype.String},
// 			&dataset.Field{Name: "gps_code", Type: datatype.String},
// 			&dataset.Field{Name: "iata_code", Type: datatype.String},
// 			&dataset.Field{Name: "local_code", Type: datatype.String},
// 			&dataset.Field{Name: "bool_teim", Type: datatype.Boolean},
// 		}},
// 			false,
// 			[][][]byte{
// 				[][]byte{[]byte("00AR"), []byte("heliport"), []byte("Newport Hospital & Clinic Heliport"), []byte{}, []byte{}, []byte{}, []byte("NA"), []byte("US"), []byte("US-AR"), []byte("Newport"), []byte("00AR"), []byte{}, []byte("00AR"), []byte{}},
// 			}, "[\n[\"00AR\",\"heliport\",\"Newport Hospital & Clinic Heliport\",0,0,0,\"NA\",\"US\",\"US-AR\",\"Newport\",\"00AR\",\"\",\"00AR\",false]\n]",
// 		},
// 	}

// 	for i, c := range cases {
// 		w := NewJsonWriter(c.ds, c.writeObjects)
// 		for _, ent := range c.entries {
// 			if err := w.WriteRow(ent); err != nil {
// 				t.Errorf("case %d WriteRow error: %s", i, err.Error())
// 				break
// 			}
// 		}
// 		if err := w.Close(); err != nil {
// 			t.Errorf("case %d Close error: %s", i, err.Error())
// 		}

// 		if string(w.Bytes()) != c.out {
// 			t.Errorf("case %d result mismatch. expected:\n%s\ngot:\n%s", i, c.out, string(w.Bytes()))
// 		}
// 	}
// }
