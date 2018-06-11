package dsfs

import (
	"fmt"
	"io"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

// LoadData loads the data this dataset points to from the store
func LoadData(store cafs.Filestore, ds *dataset.Dataset) (cafs.File, error) {
	return store.Get(datastore.NewKey(ds.BodyPath))
}

// LoadRows loads a slice of raw bytes inside a limit/offset row range
func LoadRows(store cafs.Filestore, ds *dataset.Dataset, limit, offset int) ([]byte, error) {

	datafile, err := LoadData(store, ds)
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}

	added := 0
	buf, err := dsio.NewEntryBuffer(ds.Structure)
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}

	rr, err := dsio.NewEntryReader(ds.Structure, datafile)
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}
	err = dsio.EachEntry(rr, func(i int, ent dsio.Entry, err error) error {
		if err != nil {
			log.Debugf("error reading entry: %s", err.Error())
			return err
		}

		if i < offset {
			return nil
		} else if limit > 0 && added == limit {
			return io.EOF
		}
		buf.WriteEntry(ent)
		added++
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error reading dataset data: %s", err.Error())
	}

	err = buf.Close()
	return buf.Bytes(), err
}
