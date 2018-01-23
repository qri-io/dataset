package dsfs

import (
	"fmt"
	"io"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/vals"
)

// LoadData loads the data this dataset points to from the store
func LoadData(store cafs.Filestore, ds *dataset.Dataset) (cafs.File, error) {
	return store.Get(datastore.NewKey(ds.DataPath))
}

// LoadRows loads a slice of raw bytes inside a limit/offset row range
func LoadRows(store cafs.Filestore, ds *dataset.Dataset, limit, offset int) ([]byte, error) {

	datafile, err := LoadData(store, ds)
	if err != nil {
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}

	added := 0
	buf, err := dsio.NewValueBuffer(ds.Structure)
	if err != nil {
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}

	rr, err := dsio.NewValueReader(ds.Structure, datafile)
	if err != nil {
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}
	err = dsio.EachValue(rr, func(i int, val vals.Value, err error) error {
		if err != nil {
			return err
		}

		if i < offset {
			return nil
		} else if limit > 0 && added == limit {
			return io.EOF
		}
		buf.WriteValue(val)
		added++
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error reading dataset data: %s", err.Error())
	}

	err = buf.Close()
	return buf.Bytes(), err
}
