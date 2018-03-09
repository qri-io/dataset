package validate

import (
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/jsonschema"
)

// EntryReader consumes a reader & returns any validation errors present
// TODO - refactor this to wrap a reader & return a struct that gives an
// error or nil on each entry read.
func EntryReader(r dsio.EntryReader) ([]jsonschema.ValError, error) {
	st := r.Structure()

	buf, err := dsio.NewEntryBuffer(&dataset.Structure{
		Format: dataset.JSONDataFormat,
		Schema: st.Schema,
	})
	if err != nil {
		return nil, fmt.Errorf("error allocating data buffer: %s", err.Error())
	}

	err = dsio.EachEntry(r, func(i int, ent dsio.Entry, err error) error {
		if err != nil {
			return fmt.Errorf("error reading row %d: %s", i, err.Error())
		}
		return buf.WriteEntry(ent)
	})

	if err != nil {
		return nil, fmt.Errorf("error reading values: %s", err.Error())
	}

	if e := buf.Close(); e != nil {
		return nil, fmt.Errorf("error closing buffer: %s", e.Error())
	}

	data := buf.Bytes()

	if len(data) == 0 {
		// TODO - wut?
		return nil, fmt.Errorf("err reading data")
	}

	return st.Schema.ValidateBytes(data)
}
