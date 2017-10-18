// loads dataset data from an ipfs-datastore
package load

import (
	"encoding/csv"
	"fmt"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
	"github.com/qri-io/dataset/dsio"
	"io"
)

// RowDataRows loads a slice of raw bytes inside a limit/offset row range
func RawDataRows(store cafs.Filestore, ds *dataset.Dataset, limit, offset int) ([]byte, error) {
	st := ds.Structure
	// st, err := ds.LoadStructure(store)
	// if err != nil {
	// 	return nil, err
	// }

	datafile, err := dsfs.LoadDatasetData(store, ds)
	if err != nil {
		return nil, err
	}

	added := 0
	buf := dsio.NewBuffer(st)

	err = EachRow(st, datafile, func(i int, row [][]byte, err error) error {
		if err != nil {
			return err
		} else if i < offset {
			return nil
		} else if i-offset == added {
			return fmt.Errorf("EOF")
		}

		buf.WriteRow(row)
		added++
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = buf.Close()
	return buf.Bytes(), err
}

// DataIteratorFunc is a function for each "row" of a resource's raw data
type DataIteratorFunc func(int, [][]byte, error) error

// EachRow calls fn on each row of raw data, using a structure for parsing
func EachRow(st *dataset.Structure, r io.Reader, fn DataIteratorFunc) error {
	switch st.Format {
	case dataset.CsvDataFormat:
		rdr := csv.NewReader(r)
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

func FormatRows(st *dataset.Structure, file io.Reader) (data [][][]byte, err error) {
	err = EachRow(st, file, func(_ int, row [][]byte, e error) error {
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
