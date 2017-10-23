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
	st := ds.Structure

	datafile, err := dsfs.LoadDatasetData(store, ds)
	if err != nil {
		return nil, fmt.Errorf("error loading dataset data: %s", err.Error())
	}

	added := 0
	buf := NewBuffer(st)

	err = EachRow(st, datafile, func(i int, row [][]byte, err error) error {
		if err != nil {
			return err
		}

		if i < offset {
			return nil
		} else if added == limit {
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

// EachRow calls fn on each row of raw data, using a structure for parsing
func EachRow(st *dataset.Structure, r io.Reader, fn DataIteratorFunc) error {
	rdr := NewReader(st, r)
	num := 0
	for {
		row, err := rdr.ReadRow()
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

	return fmt.Errorf("cannot parse data format '%s'", st.Format.String())
}
