// loads dataset data from an ipfs-datastore
package load

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/qri-io/castore"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/writers"
)

// RowDataRows loads a slice of raw bytes inside a limit/offset row range
func RawDataRows(store castore.Datastore, ds *dataset.Dataset, limit, offset int) ([]byte, error) {
	st := ds.Structure
	// st, err := ds.LoadStructure(store)
	// if err != nil {
	// 	return nil, err
	// }

	rawdata, err := ds.LoadData(store)
	if err != nil {
		return nil, err
	}

	added := 0
	// if st.Format != dataset.CsvDataFormat {
	// 	return nil, fmt.Errorf("raw data rows only works with csv data format for now")
	// }

	// buf := &bytes.Buffer{}
	// w := csv.NewWriter(buf)
	w := writers.NewWriter(st)

	err = EachRow(st, rawdata, func(i int, row [][]byte, err error) error {
		if err != nil {
			return err
		} else if i < offset {
			return nil
		} else if i-offset == added {
			return fmt.Errorf("EOF")
		}

		w.WriteRow(row)
		added++
		return nil
	})
	if err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// DataIteratorFunc is a function for each "row" of a resource's raw data
type DataIteratorFunc func(int, [][]byte, error) error

// EachRow calls fn on each row of raw data, using a structure for parsing
func EachRow(st *dataset.Structure, rawdata []byte, fn DataIteratorFunc) error {
	switch st.Format {
	case dataset.CsvDataFormat:
		rdr := csv.NewReader(bytes.NewReader(rawdata))
		if HeaderRow(st) {
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

	return fmt.Errorf("cannot parse data format '%s'", st.Format.String())
}

// Ugh, this shouldn't exist. re-architect around some sort of row-reader interface
func AllRows(store castore.Datastore, ds *dataset.Dataset) (data [][][]byte, err error) {
	// st, err := ds.LoadStructure(store)
	// if err != nil {
	// 	return nil, err
	// }

	rawdata, err := ds.LoadData(store)
	if err != nil {
		return nil, err
	}

	return FormatRows(ds.Structure, rawdata)
}

func FormatRows(st *dataset.Structure, rawdata []byte) (data [][][]byte, err error) {
	err = EachRow(st, rawdata, func(_ int, row [][]byte, e error) error {
		if e != nil {
			return e
		}
		data = append(data, row)
		return nil
	})
	return
}

func HeaderRow(st *dataset.Structure) bool {
	if st.Format == dataset.CsvDataFormat && st.FormatConfig != nil {
		if csvOpt, ok := st.FormatConfig.(*dataset.CsvOptions); ok {
			return csvOpt.HeaderRow
		}
	}
	return false
}

// TODO - this won't work b/c underlying implementations are different
// time to create an interface that conforms all different data types to readers & writers
// that think in terms of rows, etc.
// func NewWriter(r *dataset.Dataset) (w io.WriteCloser, buf *bytes.Buffer, err error) {
// 	buf = &bytes.Buffer{}
// 	switch r.Format {
// 	case dataset.CsvDataFormat:
// 		return csv.NewWriter(buf), buf, nil
// 	case dataset.JsonDataFormat:
// 		return nil, nil, fmt.Errorf("json writer unfinished")
// 	default:
// 		return nil, nil, fmt.Errorf("unrecognized data format for creating writer: %s", r.Format.String())
// 	}
// }

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
