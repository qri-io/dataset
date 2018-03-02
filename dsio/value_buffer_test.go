package dsio

import (
	"encoding/json"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
	"github.com/qri-io/dataset/vals"
)

func TestValueBuffer(t *testing.T) {
	tc, err := dstest.NewTestCaseFromDir("testdata/csv/movies", t)
	if err != nil {
		t.Errorf("error loading test case: %s", err.Error())
		return
	}

	ds := tc.Input
	t.Logf("%v", ds.Structure.Schema)

	outst := &dataset.Structure{
		Format: dataset.JSONDataFormat,
		Schema: ds.Structure.Schema,
	}

	rbuf, err := NewValueBuffer(outst)
	if err != nil {
		t.Errorf("error allocating ValueBuffer: %s", err.Error())
		return
	}

	rr, err := NewValueReader(ds.Structure, tc.DataFile())
	if err != nil {
		t.Errorf("error allocating RowReader: %s", err.Error())
		return
	}

	if err = EachValue(rr, func(i int, val vals.Value, err error) error {
		if err != nil {
			return err
		}
		return rbuf.WriteValue(val)
	}); err != nil {
		t.Errorf("error writing rows: %s", err.Error())
		return
	}

	bst := rbuf.Structure()
	if err := dataset.CompareStructures(outst, bst); err != nil {
		t.Errorf("buffer structure mismatch: %s", err.Error())
		return
	}

	if err := rbuf.Close(); err != nil {
		t.Errorf("error closing buffer: %s", err.Error())
		return
	}

	out := []interface{}{}
	if err := json.Unmarshal(rbuf.Bytes(), &out); err != nil {
		t.Errorf("error unmarshaling encoded bytes: %s", err.Error())
		return
	}

	if _, err = json.Marshal(out); err != nil {
		t.Errorf("error marshaling json data: %s", err.Error())
		return
	}

	// ioutil.WriteFile("testdata/movies_out.json", jsondata, 0777)
}
