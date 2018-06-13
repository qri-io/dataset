package dsio

import (
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
)

func TestEachEntry(t *testing.T) {
	tc, err := dstest.NewTestCaseFromDir("testdata/json/city")
	if err != nil {
		t.Errorf("error reading test case: %s", err.Error())
		return
	}

	st := &dataset.Structure{
		Format: dataset.JSONDataFormat,
		Schema: dataset.BaseSchemaArray,
	}
	r, err := NewEntryReader(st, tc.BodyFile())
	if err != nil {
		t.Errorf("error allocating RowReader: %s", err.Error())
		return
	}

	err = EachEntry(r, func(i int, ent Entry, err error) error {
		if err != nil {
			return err
		}

		// if len(expect[i]) != len(data) {
		// 	return fmt.Errorf("data length mismatch. expected %d, got: %d", len(expect[i]), len(data))
		// }

		// for j, cell := range data {
		// 	if !bytes.Equal(expect[i][j], cell) {
		// 		return fmt.Errorf("result mismatch. row: %d, cell: %d. %s != %s", i, j, string(expect[i][j]), string(cell))
		// 	}
		// }

		return nil
	})

	if err != nil {
		t.Errorf("eachrow error: %s", err.Error())
		return
	}
}
