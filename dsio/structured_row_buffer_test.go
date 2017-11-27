package dsio

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/qri-io/dataset"
	dmp "github.com/sergi/go-diff/diffmatchpatch"
)

func TestStructuredRowBuffer(t *testing.T) {
	datasets, err := makeTestData()
	if err != nil {
		t.Errorf("error creating filestore", err.Error())
		return
	}

	cases := []struct {
		dsName     string
		Cfg        func(cfg *StructuredRowBufferCfg)
		resultPath string
		// newErr     string
	}{
		{"movies", func(cfg *StructuredRowBufferCfg) {}, "testdata/movies.csv"},
		{"movies", func(cfg *StructuredRowBufferCfg) {
			cfg.OrderBy = []*dataset.Field{
				&dataset.Field{Name: "movie_title"},
			}
		}, "testdata/movies_sorted_movie_title.csv"},
	}

	for i, c := range cases {
		ds := datasets[c.dsName].ds
		// if err != nil {
		// 	t.Errorf("error creating dataset: %s", err.Error())
		// 	return
		// }

		// outst := &dataset.Structure{
		// 	Format: dataset.JSONDataFormat,
		// 	FormatConfig: &dataset.JSONOptions{
		// 		ArrayEntries: false,
		// 	},
		// 	Schema: ds.Structure.Schema,
		// }

		srbuf, err := NewStructuredRowBuffer(ds.Structure, c.Cfg)
		if err != nil {
			t.Errorf("case %d error allocating StructuredRowBuffer: %s", i, err.Error())
			continue
		}

		rr, err := NewRowReader(ds.Structure, bytes.NewBuffer(datasets["movies"].data))
		if err != nil {
			t.Errorf("case %d error allocating RowReader: %s", i, err.Error())
			continue
		}

		if err = EachRow(rr, func(i int, row [][]byte, err error) error {
			if err != nil {
				return err
			}
			return srbuf.WriteRow(row)
		}); err != nil {
			t.Errorf("error writing rows: %s", err.Error())
			continue
		}

		srbufs := srbuf.Structure()
		if err := dataset.CompareStructures(ds.Structure, srbufs); err != nil {
			t.Errorf("case %d buffer structure mismatch: %s", i, err.Error())
			continue
		}

		if err := srbuf.Close(); err != nil {
			t.Errorf("case %d error closing buffer: %s", i, err.Error())
			continue
		}

		if c.resultPath != "" {
			expectBytes, err := ioutil.ReadFile(c.resultPath)
			if err != nil {
				t.Errorf("case %d error reading result data file: %s", i, err.Error())
				continue
			}
			if !bytes.Equal(expectBytes, srbuf.Bytes()) {
				dmp := dmp.New()
				diffs := dmp.DiffMain(string(expectBytes), string(srbuf.Bytes()), true)
				if len(diffs) == 0 {
					t.Logf("case %d bytes were unequal but computed no difference between results")
					continue
				}

				t.Errorf("case %d mismatch:\n%s", i, dmp.DiffPrettyText(diffs))
				path := fmt.Sprintf("%sTestStructuredRowBuffer_case_%d_data.%s", os.TempDir(), i, ds.Structure.Format.String())
				if err := ioutil.WriteFile(path, srbuf.Bytes(), os.ModePerm); err == nil {
					t.Logf("result bytes written to: %s", path)
				}
			}
		}

	}

	// out := []interface{}{}
	// if err := json.Unmarshal(rbuf.Bytes(), &out); err != nil {
	// 	t.Errorf("error unmarshaling encoded bytes: %s", err.Error())
	// 	return
	// }

	// if _, err = json.Marshal(out); err != nil {
	// 	t.Errorf("error marshaling json data: %s", err.Error())
	// 	return
	// }
}
