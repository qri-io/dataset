// load-dataset (packaged as just "load") loads dataset from an ipfs-datastore
package load

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset"
	// "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

// LoadResource loads a resource from a store
func LoadResource(store datastore.Datastore, path datastore.Key) (*dataset.Resource, error) {
	v, err := store.Get(path)
	if err != nil {
		return nil, err
	}

	return dataset.UnmarshalResource(v)
}

// DataIteratorFunc is a function for each "row" of a resource's raw data
type DataIteratorFunc func(int, [][]byte, error) error

// EachRow
func EachRow(r *dataset.Resource, rawdata []byte, fn DataIteratorFunc) error {
	switch r.Format {
	case dataset.CsvDataFormat:
		rdr := csv.NewReader(bytes.NewReader(rawdata))
		if HeaderRow(r) {
			if _, err := rdr.Read(); err != nil {
				if err.Error() == "EOF" {
					return nil
				}
				return err
			}
		}

		num := 1
		for {
			csvRec, err := rdr.Read()
			if err != nil {
				if err.Error() == "EOF" {
					return nil
				}
				return err
			}

			rec := make([][]byte, len(csvRec))
			for i, col := range csvRec {
				rec[i] = []byte(col)
			}

			if err := fn(num, rec, err); err != nil {
				if err.Error() == "EOF" {
					return nil
				}
				return err
			}
			num++
		}
		// case dataset.JsonDataFormat:
	}

	return fmt.Errorf("cannot parse data format '%s'", r.Format.String())
}

// Ugh, this shouldn't exist. re-architect around some sort of row-reader interface
func AllRows(store datastore.Datastore, r *dataset.Resource) (data [][][]byte, err error) {
	d, err := store.Get(r.Path)
	rawdata, ok := d.([]byte)
	if !ok {
		return nil, fmt.Errorf("resource data should be a slice of bytes")
	}

	err = EachRow(r, rawdata, func(_ int, row [][]byte, e error) error {
		if e != nil {
			return e
		}
		data = append(data, row)
		return nil
	})

	return
}

func HeaderRow(r *dataset.Resource) bool {
	if r.Format == dataset.CsvDataFormat && r.FormatConfig != nil {
		if csvOpt, ok := r.FormatConfig.(*dataset.CsvOptions); ok {
			return csvOpt.HeaderRow
		}
	}
	return false
}

// FetchBytes grabs the actual byte data that this dataset represents
// it is expected that the passed-in store will be scoped to the dataset
// itself
// func (r *Dataset) FetchBytes(store fs.Store) ([]byte, error) {
// 	if len(r.Data) > 0 {
// 		return r.Data, nil
// 	} else if r.File != "" {
// 		// return store.Read(r.Address.PathString(r.File))
// 		return store.Read(r.File)
// 	} else if r.Url != "" {
// 		res, err := http.Get(r.Url)
// 		if err != nil {
// 			return nil, err
// 		}

// 		defer res.Body.Close()
// 		return ioutil.ReadAll(res.Body)
// 	}

// 	return nil, fmt.Errorf("dataset '%s' doesn't contain a url, file, or data field to read from", r.Name)
// }

// func (r *Dataset) Reader(store fs.Store) (io.ReadCloser, error) {
// 	if len(r.Data) > 0 {
// 		return ioutil.NopCloser(bytes.NewBuffer(r.Data)), nil
// 	} else if r.File != "" {
// 		return store.Open(r.File)
// 	} else if r.Url != "" {
// 		res, err := http.Get(r.Url)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return res.Body, nil
// 	}
// 	return nil, fmt.Errorf("dataset %s doesn't contain a url, file, or data field to read from", r.Name)
// }
