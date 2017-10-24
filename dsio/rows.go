package dsio

import (
	"fmt"
	"io"

	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
)

// DataIteratorFunc is a function for each "row" of a resource's raw data
type DataIteratorFunc func(int, [][]byte, error) error

// ReadRows loads a slice of raw bytes inside a limit/offset row range
func ReadRows(store cafs.Filestore, ds *dataset.Dataset, limit, offset int) ([]byte, error) {

	datafile, err := dsfs.LoadDatasetData(store, ds)
	if err != nil {
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}

	added := 0
	buf := NewBuffer(ds.Structure)
	r := NewRowReader(ds.Structure, datafile)

	err = EachRow(r, func(i int, row [][]byte, err error) error {
		if err != nil {
			return err
		}

		if i < offset {
			return nil
		} else if limit > 0 && added == limit {
			return io.EOF
		}

		buf.WriteRow(row)
		added++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error iterating through dataset data: %s", err.Error())
	}

	err = buf.Close()
	return buf.Bytes(), err
}

// EachRow calls fn on each row of a given RowReader
func EachRow(rr RowReader, fn DataIteratorFunc) error {
	num := 0
	for {
		row, err := rr.ReadRow()
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return fmt.Errorf("error reading record: %s", err.Error())
		}

		if err := fn(num, row, err); err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
		num++
	}

	return fmt.Errorf("cannot parse data format '%s'", rr.Structure().Format.String())
}
