package preview

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/qfs"
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

// Create generates a preview for a dataset version
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
func Create(ctx context.Context, ds *dataset.Dataset) (*dataset.Dataset, error) {
	var (
		err error
		p   = &dataset.Dataset{}
	)

	if ds == nil {
		log.Debugf("Create: nil dataset")
		return nil, fmt.Errorf("nil dataset")
	}
	if ds.IsEmpty() {
		log.Debugf("Create: empty dataset")
		return nil, fmt.Errorf("empty dataset")
	}

	p.Assign(ds)

	if ds.Readme != nil && ds.Readme.ScriptFile() != nil {
		buf := &bytes.Buffer{}
		f := ds.Readme.ScriptFile()
		tr := io.TeeReader(f, buf)

		ds.Readme.ScriptBytes, err = ioutil.ReadAll(io.LimitReader(tr, MaxReadmePreviewBytes))
		if err != nil {
			log.Errorf("Reading Readme: %s", err.Error())
			return nil, err
		}

		if len(ds.Readme.ScriptBytes) == MaxReadmePreviewBytes {
			ds.Readme.ScriptBytes = append(ds.Readme.ScriptBytes, []byte(`...`)...)
		}
		ds.Readme.SetScriptFile(qfs.NewMemfileReader(f.FullPath(), io.MultiReader(buf, f)))
	}

	if ds.BodyFile() != nil {
		st := &dataset.Structure{
			Format: "json",
			Schema: ds.Structure.Schema,
		}

		buf := &bytes.Buffer{}
		f := ds.BodyFile()
		tr := io.TeeReader(f, buf)
		teedFile := qfs.NewMemfileReader(f.FullPath(), tr)

		data, err := dsio.ConvertFile(teedFile, ds.Structure, st, MaxNumDatasetRowsInPreview, 0, false)
		if err != nil {
			log.Errorf("Create converting body file: %s", err.Error())
			return nil, err
		}

		ds.Body = json.RawMessage(data)
		ds.SetBodyFile(qfs.NewMemfileReader(f.FullPath(), io.MultiReader(buf, f)))
	}

	// TODO (b5) - previews currently don't include the new stats component, because
	// we don't have logic for dropping the space-intensive fields in stat structs
	// Once we have a clear way to drop things like string frequency counts, and can
	// get the byte-cost of stats to scale linearly with the dataset column count
	// previews should include stats
	ds.Stats = nil

	return ds, nil
}
