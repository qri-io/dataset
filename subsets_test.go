package dataset

import (
	"encoding/json"
	"fmt"

	"testing"
)

func SubsetsEqual(a, b Subsets) error {
	if a.SubsetsFolder != b.SubsetsFolder {
		return fmt.Errorf("folder mismatch: %s != %s", a.SubsetsFolder, b.SubsetsFolder)
	}
	if len(a.Datasets) != len(b.Datasets) {
		return fmt.Errorf("dataset length mismatch: %d != %d", len(a.Datasets), len(b.Datasets))
	}

	for i, ds := range a.Datasets {
		if err := DatasetEqual(ds, b.Datasets[i]); err != nil {
			return fmt.Errorf("dataset %d mismatch: %s", i, err)
		}
	}

	return nil
}

func TestSubsetsUnmarshalJSON(t *testing.T) {
	cases := []struct {
		str      string
		datasets Subsets
		err      error
	}{
		{`"datasets"`, Subsets{SubsetsFolder: "datasets"}, nil},
		{`[{"address":"a"},{"address":"b"}]`, Subsets{Datasets: []*Dataset{&Dataset{Address: NewAddress("a")}, &Dataset{Address: NewAddress("b")}}}, nil},
	}

	for i, c := range cases {
		got := Subsets{}
		if err := json.Unmarshal([]byte(c.str), &got); err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if err := SubsetsEqual(c.datasets, got); err != nil {
			t.Errorf("case %d datasets mismatch: %s", i, err)
			continue
		}
	}
}

func TestSubsetsMarshalJSON(t *testing.T) {
	cases := []struct {
		ds  Subsets
		out string
		err error
	}{
		{Subsets{SubsetsFolder: `"datasets"`}, `"datasets"`, nil},
		{Subsets{SubsetsFolder: "", Datasets: []*Dataset{&Dataset{Address: NewAddress("a")}, &Dataset{Address: NewAddress("b")}}}, `[{"address":"a"},{"address":"b"}]`, nil},
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
