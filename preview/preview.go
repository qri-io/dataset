package preview

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

var (
	log = logger.Logger("preview")
)

const (
	// MaxNumDatasetRowsInPreview is the highest number of rows a dataset preview
	// can contain
	MaxNumDatasetRowsInPreview = 100
	// MaxReadmePreviewBytes determines the maximum amount of bytes a readme
	// preview can be. three bytes less than 1000 to make room for an elipsis
	MaxReadmePreviewBytes = 997
)

// CreatePreview generates a preview for a dataset version
// It expects the passed in dataset to have any relevant script files already
// loaded
// Preview currently includes:
//    - body: 100 rows
//    - readme: first 997 bytes
//    - meta: all
//    - commit: all
//    - structure: all
//    - stats: none
//    - viz: all
//    - transform: all
func CreatePreview(ctx context.Context, ds *dataset.Dataset) (*dataset.Dataset, error) {
	var err error

	if ds == nil {
		log.Debugf("CreatePreview: nil dataset")
		return nil, fmt.Errorf("nil dataset")
	}

	if ds.IsEmpty() {
		log.Debugf("CreatePreview: empty dataset")
		return nil, fmt.Errorf("empty dataset")
	}

	if ds.Readme != nil && ds.Readme.ScriptFile() != nil {
		ds.Readme.ScriptBytes, err = ioutil.ReadAll(io.LimitReader(ds.Readme.ScriptFile(), MaxReadmePreviewBytes))
		if err != nil {
			log.Errorf("Reading Readme: %s", err.Error())
			return nil, err
		}

		if len(ds.Readme.ScriptBytes) == MaxReadmePreviewBytes {
			ds.Readme.ScriptBytes = append(ds.Readme.ScriptBytes, []byte(`...`)...)
		}
		ds.Readme.SetScriptFile(nil)
	}

	if ds.BodyFile() != nil {
		st := &dataset.Structure{
			Format: "json",
			Schema: ds.Structure.Schema,
		}

		data, err := dsio.ConvertFile(ds.BodyFile(), ds.Structure, st, MaxNumDatasetRowsInPreview, 0, false)
		if err != nil {
			log.Errorf("CreatePreview converting body file: %s", err.Error())
			return nil, err
		}

		ds.Body = json.RawMessage(data)
	}

	// TODO (b5) - previews currently don't include the new stats component, because
	// we don't have logic for dropping the space-intensive fields in stat structs
	// Once we have a clear way to drop things like string frequency counts, and can
	// get the byte-cost of stats to scale linearly with the dataset column count
	// previews should include stats
	ds.Stats = nil

	return ds, nil
}
