package dsio

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
)

func makeFilestore() (map[string]datastore.Key, cafs.Filestore, error) {
	fs := memfs.NewMapstore()

	datasets := map[string]datastore.Key{
		"movies": datastore.NewKey(""),
		"cities": datastore.NewKey(""),
	}

	for k, _ := range datasets {
		rawdata, err := ioutil.ReadFile(fmt.Sprintf("testdata/%s.csv", k))
		if err != nil {
			return datasets, nil, err
		}

		datakey, err := fs.Put(memfs.NewMemfileBytes(k, rawdata), true)
		if err != nil {
			return datasets, nil, err
		}

		dsdata, err := ioutil.ReadFile(fmt.Sprintf("testdata/%s.json", k))
		if err != nil {
			return datasets, nil, err
		}

		ds := &dataset.Dataset{}
		if err := json.Unmarshal(dsdata, ds); err != nil {
			return datasets, nil, err
		}
		ds.Data = datakey
		dskey, err := dsfs.SaveDataset(fs, ds, true)
		if err != nil {
			return datasets, nil, err
		}
		datasets[k] = dskey
	}

	return datasets, fs, nil
}
