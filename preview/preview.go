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

	if ds == nil || ds.IsEmpty() {
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

	if ds.BodyFile() == nil {
		return nil, fmt.Errorf("no body file")
	}

	st := &dataset.Structure{
		Format: "json",
		Schema: ds.Structure.Schema,
	}

	data, err := ConvertBodyFile(ds.BodyFile(), ds.Structure, st, MaxNumDatasetRowsInPreview, 0, false)
	if err != nil {
		log.Errorf("CreatePreview converting body file: %s", err.Error())
		return nil, err
	}

	// TODO (b5) - previews currently don't include the new stats component, because
	// we don't have logic for dropping the space-intensive fields in stat structs
	// Once we have a clear way to drop things like string frequency counts, and can
	// get the byte-cost of stats to scale linearly with the dataset column count
	// previews should include stats
	ds.Stats = nil

	ds.Body = json.RawMessage(data)
	return ds, nil
}

// ConvertBodyFile takes an input file & structure, and converts a specified selection
// to the structure specified by out
func ConvertBodyFile(file qfs.File, in, out *dataset.Structure, limit, offset int, all bool) (data []byte, err error) {
	buf := &bytes.Buffer{}

	w, err := dsio.NewEntryWriter(out, buf)
	if err != nil {
		return
	}

	// TODO(dlong): Kind of a hacky one-off. Generalize this for other format options.
	if out.DataFormat() == dataset.JSONDataFormat {
		ok, pretty := out.FormatConfig["pretty"].(bool)
		if ok && pretty {
			w, err = dsio.NewJSONPrettyWriter(out, buf, " ")
		}
	}
	if err != nil {
		return
	}

	rr, err := dsio.NewEntryReader(in, file)
	if err != nil {
		err = fmt.Errorf("error allocating data reader: %s", err)
		return
	}

	if !all {
		rr = &dsio.PagedReader{
			Reader: rr,
			Limit:  limit,
			Offset: offset,
		}
	}
	err = dsio.Copy(rr, w)

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("error closing row buffer: %s", err.Error())
	}

	return buf.Bytes(), nil
}
