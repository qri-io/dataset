package dataset

import (
	"encoding/json"
	"errors"
)

// datasource outlines the formal definition for being a source of data
type DataSource struct {
	// at most one of these can be set
	Url  string `json:"url,omitempty"`
	File string `json:"file,omitempty"`
	Data []byte `json:"data,omitempty"`

	// This guy is required if data is going to be set
	Format DataFormat `json:"format,omitempty"`

	// This stuff defines the 'schema' for a dataset's data
	Fields     []*Field `json:"fields,omitempty"`
	PrimaryKey FieldKey `json:"primaryKey,omitempty"`

	// optional-but-sometimes-necessary info
	Mediatype string `json:"mediatype,omitempty"`
	Encoding  string `json:"encoding,omitempty"`
	Bytes     int    `json:"bytes,omitempty"`
	Hash      string `json:"hash,omitempty"`
}

// underlying type for marshalling into
type _dataSource DataSource

// truthCount returns the number of arguments that are true
func truthCount(args ...bool) (count int) {
	for _, arg := range args {
		if arg {
			count++
		}
	}
	return
}

// UnmarhalJSON can marshal in two forms: just an id string, or an object containing a full data model
func (d *DataSource) UnmarshalJSON(data []byte) error {
	ds := _dataSource{}
	if err := json.Unmarshal(data, &ds); err != nil {
		return err
	}

	if count := truthCount(ds.Url != "", ds.File != "", len(ds.Data) > 0); count > 1 {
		return errors.New("only one of url, file, or data can be set")
	} else if count == 1 {
		if ds.Format == UnknownDataFormat {
			return errors.New("format is required for data source")
		}
	}

	*d = DataSource(ds)
	return nil
}
