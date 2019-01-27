package dsio

import (
	"fmt"
	"io"
)

// Entry is a "row" of a dataset
type Entry struct {
	// Index represents this entry's numeric position in a dataset
	// this index may not necessarily refer to the overall position within the dataset
	// as things like offsets affect where the index begins
	Index int
	// Key is a string key for this entry
	// only present when the top level structure is a map
	Key string
	// Value is information contained within the row
	Value interface{}
}

// DataIteratorFunc is a function for each "row" of a resource's raw data
type DataIteratorFunc func(int, Entry, error) error

// EachEntry calls fn on each row of a given EntryReader
func EachEntry(rr EntryReader, fn DataIteratorFunc) error {
	num := 0
	for {
		row, err := rr.ReadEntry()
		if err != nil {
			if err.Error() == io.EOF.Error() {
				return nil
			}
			err := fmt.Errorf("error reading row %d: %s", num, err.Error())
			log.Debug(err.Error())
			return err
		}

		if err := fn(num, row, err); err != nil {
			if err.Error() == io.EOF.Error() {
				return nil
			}
			log.Debug(err.Error())
			return err
		}
		num++
	}

}
