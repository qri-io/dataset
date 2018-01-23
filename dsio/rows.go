package dsio

import (
	"fmt"
	"io"

	"github.com/qri-io/dataset/vals"
)

// DataIteratorFunc is a function for each "row" of a resource's raw data
type DataIteratorFunc func(int, vals.Value, error) error

// EachValue calls fn on each row of a given ValueReader
func EachValue(rr ValueReader, fn DataIteratorFunc) error {
	num := 0
	for {
		row, err := rr.ReadValue()
		if err != nil {
			if err.Error() == io.EOF.Error() {
				return nil
			}
			return fmt.Errorf("error reading row: %s", err.Error())
		}

		if err := fn(num, row, err); err != nil {
			if err.Error() == io.EOF.Error() {
				return nil
			}
			return err
		}
		num++
	}

	return fmt.Errorf("cannot parse data format '%s'", rr.Structure().Format.String())
}
