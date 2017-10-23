package dsio

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/qri-io/dataset/dsfs"
)

func TestRawDataRows(t *testing.T) {
	datasets, store, err := makeFilestore()
	if err != nil {
		t.Errorf("error creating test filestore: %s", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(store, datasets["cities"])
	if err != nil {
		t.Errorf("error loading dataset: %s", err.Error())
		return
	}

	data, err := ReadRows(store, ds, 2, 2)
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
	datasets, store, err := makeFilestore()
	if err != nil {
		t.Errorf("error creating test filestore: %s", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(store, datasets["cities"])
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
