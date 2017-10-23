package dsio

import (
	"encoding/json"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
)

func TestBuffer(t *testing.T) {
	dskey, fs, err := makeFilestore()
	if err != nil {
		t.Errorf("error creating filestore", err.Error())
		return
	}

	ds, err := dsfs.LoadDataset(fs, dskey)
	if err != nil {
		t.Errorf("error creating dataset: %s", err.Error())
		return
	}

	outst := &dataset.Structure{
		Format: dataset.JsonDataFormat,
		FormatConfig: &dataset.JsonOptions{
			ObjectEntries: true,
		},
		Schema: ds.Structure.Schema,
	}

	buf := NewBuffer(outst)
	dsfile, err := dsfs.LoadDatasetData(fs, ds)
	if err != nil {
		t.Errorf("error reading dataset file: %s", err.Error())
		return
	}

	// r := NewReader(ds.Structure, dsfile)
	err = EachRow(ds.Structure, dsfile, func(i int, row [][]byte, err error) error {
		if err != nil {
			return err
		}
		return buf.WriteRow(row)
	})

	if err != nil {
		t.Errorf("error iterating through rows: %s", err.Error())
		return
	}

	if err := buf.Close(); err != nil {
		t.Errorf("error closing buffer: %s", err.Error())
		return
	}

	out := []interface{}{}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Errorf("error unmarshaling encoded bytes: %s", err.Error())
		return
	}
}
