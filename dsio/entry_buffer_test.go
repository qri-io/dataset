package dsio

import (
	"encoding/json"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
)

func TestEntryBuffer(t *testing.T) {
	tc, err := dstest.NewTestCaseFromDir("testdata/csv/movies")
	if err != nil {
		t.Errorf("error loading test case: %s", err.Error())
		return
	}

	ds := tc.Input

	outst := &dataset.Structure{
		Format: "json",
		Schema: ds.Structure.Schema,
	}

	rbuf, err := NewEntryBuffer(outst)
	if err != nil {
		t.Errorf("error allocating EntryBuffer: %s", err.Error())
		return
	}

	rr, err := NewEntryReader(ds.Structure, tc.BodyFile())
	if err != nil {
		t.Errorf("error allocating RowReader: %s", err.Error())
		return
	}

	if err = EachEntry(rr, func(i int, val Entry, err error) error {
		if err != nil {
			return err
		}
		return rbuf.WriteEntry(val)
	}); err != nil {
		t.Errorf("error writing rows: %s", err.Error())
		return
	}

	bst := rbuf.Structure()
	if diff := dstest.CompareStructures(outst, bst); diff != "" {
		t.Errorf("buffer structure mismatch (-wnt +got):\n%s", diff)
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
}
