package dataset

import (
	"encoding/json"
	"fmt"

	"testing"
)

func DatasetsEqual(a, b Datasets) error {
	if a.folder != b.folder {
		return fmt.Errorf("folder mismatch: %s != %s", a.folder, b.folder)
	}
	if len(a.datasets) != len(b.datasets) {
		return fmt.Errorf("dataset length mismatch: %d != %d", len(a.datasets), len(b.datasets))
	}

	for i, ds := range a.datasets {
		if err := DatasetEqual(ds, b.datasets[i]); err != nil {
			return fmt.Errorf("dataset %d mismatch: %s", i, err)
		}
	}

	return nil
}

func TestDatasetsUnmarshalJSON(t *testing.T) {
	cases := []struct {
		str      string
		datasets Datasets
		err      error
	}{
		{`"datasets"`, Datasets{folder: "datasets"}, nil},
		{`[{"address":"a"},{"address":"b"}]`, Datasets{datasets: []*Dataset{&Dataset{Address: NewAddress("a")}, &Dataset{Address: NewAddress("b")}}}, nil},
	}

	for i, c := range cases {
		got := Datasets{}
		if err := json.Unmarshal([]byte(c.str), &got); err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if err := DatasetsEqual(c.datasets, got); err != nil {
			t.Errorf("case %d datasets mismatch: %s", i, err)
			continue
		}
	}
}

func TestDatasetsMarshalJSON(t *testing.T) {
	cases := []struct {
		ds  Datasets
		out string
		err error
	}{
		{Datasets{folder: `"datasets"`}, `"datasets"`, nil},
		{Datasets{folder: "", datasets: []*Dataset{&Dataset{Address: NewAddress("a")}, &Dataset{Address: NewAddress("b")}}}, `[{"address":"a"},{"address":"b"}]`, nil},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.ds)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if string(data) != c.out {
			t.Errorf("case %d result mismatch. expected: %s, got: %s", i, c.out, string(data))
			continue
		}
	}
}
