package validate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/jsonschema"
)

const batchSize = 5000

func flushBatch(ctx context.Context, buf *dsio.EntryBuffer, st *dataset.Structure, jsch *jsonschema.Schema, errs *[]jsonschema.KeyError) error {
	if len(buf.Bytes()) == 0 {
		return nil
	}

	if e := buf.Close(); e != nil {
		return fmt.Errorf("error closing buffer: %s", e.Error())
	}

	var doc interface{}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		return fmt.Errorf("error parsing JSON bytes: %s", err.Error())
	}
	validationState := jsch.Validate(ctx, doc)
	*errs = append(*errs, *validationState.Errs...)

	return nil
}

// EntryReader consumes a reader & returns any validation errors present
// TODO - refactor this to wrap a reader & return a struct that gives an
// error or nil on each entry read.
func EntryReader(r dsio.EntryReader) ([]jsonschema.KeyError, error) {
	ctx := context.Background()
	st := r.Structure()

	jsch, err := st.JSONSchema()
	if err != nil {
		return nil, err
	}

	valErrors := []jsonschema.KeyError{}

	buf, err := dsio.NewEntryBuffer(&dataset.Structure{
		Format: "json",
		Schema: st.Schema,
	})
	if err != nil {
		return nil, fmt.Errorf("error allocating data buffer: %s", err.Error())
	}

	err = dsio.EachEntry(r, func(i int, ent dsio.Entry, err error) error {
		if err != nil {
			return fmt.Errorf("error reading row %d: %s", i, err.Error())
		}

		if i%batchSize == 0 {
			flushErr := flushBatch(ctx, buf, st, jsch, &valErrors)
			if flushErr != nil {
				return flushErr
			}
			var bufErr error
			buf, bufErr = dsio.NewEntryBuffer(&dataset.Structure{
				Format: "json",
				Schema: st.Schema,
			})
			if bufErr != nil {
				return fmt.Errorf("error allocating data buffer: %s", bufErr.Error())
			}
		}

		err = buf.WriteEntry(ent)
		if err != nil {
			return fmt.Errorf("error writing row %d: %s", i, err.Error())
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error reading values: %s", err.Error())
	}

	if err := flushBatch(ctx, buf, st, jsch, &valErrors); err != nil {
		return nil, err
	}

	return valErrors, nil
}
