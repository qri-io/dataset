package load

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/datatypes"
	"github.com/qri-io/dataset/dsfs"
)

func TestRawDataRows(t *testing.T) {
	dskey, store, err := makeFilestore()
	if err != nil {
		t.Errorf("error creating test filestore: %s", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(store, dskey)
	if err != nil {
		t.Errorf("error loading dataset: %s", err.Error())
		return
	}

	data, err := RawDataRows(store, ds, 2, 2)
	if err != nil {
		t.Errorf("raw data row error: %s", err.Error())
		return
	}

	expect := []byte(`chicago,300000,44.4,true
chatham,35000,65.25,true
`)

	if !bytes.Equal(expect, data) {
		t.Errorf("data mismatch. expected: %s, got: %s", string(expect), string(data))
		return
	}
}

func TestEachRow(t *testing.T) {
	dskey, store, err := makeFilestore()
	if err != nil {
		t.Errorf("error creating test filestore: %s", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(store, dskey)
	if err != nil {
		t.Errorf("error loading dataset: %s", err.Error())
		return
	}

	file, err := dsfs.LoadDatasetData(store, ds)
	if err != nil {
		t.Errorf("error loading dataset data: %s", err.Error())
		return
	}

	expect := [][][]byte{
		[][]byte{[]byte("toronto"), []byte("40000000"), []byte("55.5"), []byte("false")},
		[][]byte{[]byte("new york"), []byte("8500000"), []byte("44.4"), []byte("true")},
		[][]byte{[]byte("chicago"), []byte("300000"), []byte("44.4"), []byte("true")},
		[][]byte{[]byte("chatham"), []byte("35000"), []byte("65.25"), []byte("true")},
		[][]byte{[]byte("raleigh"), []byte("250000"), []byte("50.65"), []byte("true")},
	}

	err = EachRow(ds.Structure, file, func(i int, data [][]byte, err error) error {
		if err != nil {
			return err
		}
		// fmt.Println(i, len(data), string(data[0]))

		if len(expect[i]) != len(data) {
			return fmt.Errorf("data length mismatch. expected %d, got: %d", len(expect[i]), len(data))
		}

		for j, cell := range data {
			if !bytes.Equal(expect[i][j], cell) {
				return fmt.Errorf("result mismatch. row: %d, cell: %d. %s != %s", i, j, string(expect[i][j]), string(cell))
			}
		}

		return nil
	})
	if err != nil {
		t.Errorf("eachrow error: %s", err.Error())
		return
	}
}

func makeFilestore() (datastore.Key, cafs.Filestore, error) {
	const testCsvData = `city,pop,avg_age,in_usa
toronto,40000000,55.5,false
new york,8500000,44.4,true
chicago,300000,44.4,true
chatham,35000,65.25,true
raleigh,250000,50.65,true
`

	var dskey datastore.Key
	fs := memfs.NewMapstore()
	datakey, err := fs.Put(memfs.NewMemfileBytes("data.csv", []byte(testCsvData)), true)
	if err != nil {
		return dskey, nil, err
	}

	ds := &dataset.Dataset{
		Title: "example city data",
		Structure: &dataset.Structure{
			Format: dataset.CsvDataFormat,
			FormatConfig: &dataset.CsvOptions{
				HeaderRow: true,
			},
			Schema: &dataset.Schema{
				Fields: []*dataset.Field{
					&dataset.Field{Name: "city", Type: datatypes.String},
					&dataset.Field{Name: "pop", Type: datatypes.Integer},
					&dataset.Field{Name: "avg_age", Type: datatypes.Float},
					&dataset.Field{Name: "in_usa", Type: datatypes.Boolean},
				},
			},
		},
		Data: datakey,
	}

	dskey, err = dsfs.SaveDataset(fs, ds, true)
	if err != nil {
		return dskey, nil, err
	}

	return dskey, fs, nil
}
