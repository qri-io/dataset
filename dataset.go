package dataset

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/qri-io/fs"
)

const Filename = "dataset.json"

var ErrNotFound = errors.New("Not Found")

type Dataset struct {
	// not required, but if it's here, it's gotta match the base of path
	Name string `json:"name,omitempty"`
	// required for use with other datasets. a dataset's name is the base of this path
	Address Address `json:"address,omitempty"`

	// at most one of url/file/data can be set
	Url  string `json:"url,omitempty"`
	File string `json:"file,omitempty"`
	Data []byte `json:"data,omitempty"`
	// This guy is required if data is going to be set
	Format DataFormat `json:"format,omitempty"`
	// Fields & PrimaryKey define the 'schema' for a dataset's data
	Fields     []*Field `json:"fields,omitempty"`
	PrimaryKey FieldKey `json:"primaryKey,omitempty"`
	// An optional query that's used to calculate this dataset
	Query *Query `json:"query,omitempty"`
	// optional-but-sometimes-necessary info
	Mediatype string `json:"mediatype,omitempty"`
	Encoding  string `json:"encoding,omitempty"`
	Bytes     int    `json:"bytes,omitempty"`
	Hash      string `json:"hash,omitempty"`

	// A dataset can have child datasets
	Datasets []*Dataset `json:"datasets,omitempty"`

	// optional stufffff
	Author       *Person   `json:"author,omitempty"`
	Title        string    `json:"title,omitempty"`
	Image        string    `json:"image,omitempty"`
	Description  string    `json:"description,omitempty"`
	Homepage     string    `json:"homepage,omitempty"`
	IconImage    string    `json:"iconImage,omitempty"`
	PosterImage  string    `json:"posterImage,omitempty"`
	License      *License  `json:"license,omitempty"`
	Version      Version   `json:"version,omitempty"`
	Keywords     []string  `json:"keywords,omitempty"`
	Contributors []*Person `json:"contributors,omitempty"`
	Sources      []*Source `json:"sources,omitempty"`
}

func (d *Dataset) FieldNames() (names []string) {
	names = make([]string, len(d.Fields))
	for i, f := range d.Fields {
		names[i] = f.Name
	}
	return
}

func (d *Dataset) FieldForName(name string) *Field {
	for _, f := range d.Fields {
		if f.Name == name {
			return f
		}
	}
	return nil
}

func (d *Dataset) FieldTypeStrings() (types []string) {
	types = make([]string, len(d.Fields))
	for i, f := range d.Fields {
		types[i] = f.Type.String()
	}
	return
}

// FetchBytes grabs the actual byte data that this dataset represents
// path is the path to the datapackage, and only needed if using the "path"
// dataset param
func (r *Dataset) FetchBytes(store fs.Store) ([]byte, error) {
	if len(r.Data) > 0 {
		return r.Data, nil
	} else if r.File != "" {
		return store.Read(r.File)
	} else if r.Url != "" {
		res, err := http.Get(r.Url)
		if err != nil {
			return nil, err
		}

		defer res.Body.Close()
		return ioutil.ReadAll(res.Body)
	}

	return nil, fmt.Errorf("dataset '%s' doesn't contain a url, file, or data field to read from", r.Name)
}

func (r *Dataset) Reader(store fs.Store) (io.ReadCloser, error) {
	if len(r.Data) > 0 {
		return ioutil.NopCloser(bytes.NewBuffer(r.Data)), nil
	} else if r.File != "" {
		return store.Open(r.File)
	} else if r.Url != "" {
		res, err := http.Get(r.Url)
		if err != nil {
			return nil, err
		}
		return res.Body, nil
	}
	return nil, fmt.Errorf("dataset %s doesn't contain a url, file, or data field to read from", r.Name)
}

type dataWriter struct {
	buffer  *bytes.Buffer
	onClose func([]byte)
}

func (w dataWriter) Write(p []byte) (n int, err error) {
	return w.Write(p)
}

func (w dataWriter) Close() error {
	data, err := json.Marshal(w.buffer.Bytes())
	if err != nil {
		w.onClose(data)
	}
	return err
}

func (r *Dataset) Writer(src fs.Store) (io.WriteCloser, error) {
	if len(r.Data) > 0 {
		return dataWriter{buffer: bytes.NewBuffer(r.Data), onClose: func(data []byte) { r.Data = data }}, nil
	} else if r.File != "" {
		return src.Create(r.File)
	} else if r.Url != "" {
		return nil, fmt.Errorf("can't write to url-based dataset: %s", r.Url)
	}

	return nil, fmt.Errorf("dataset %s doesn't contain a path or data field to write to", r.Name)
}

func (r *Dataset) WriteData(src fs.Store, data []byte) error {
	if r.File != "" {
		return src.Write(r.File, data)
	} else if r.Url != "" {
		return fmt.Errorf("can't write to url-based dataset: %s", r.Url)
	} else {
		r.Data = data
		return nil
	}
}

// truthCount returns the number of arguments that are true
func truthCount(args ...bool) (count int) {
	for _, arg := range args {
		if arg {
			count++
		}
	}
	return
}

// separate type for marshalling into
type _dataset Dataset

// MarshalJSON makes dataset a json Marshaler, allowing datasets to be passed
// around the json.Marshaler interface
func (d Dataset) MarshalJSON() (data []byte, err error) {
	return json.Marshal(_dataset(d))
}

// UnmarshalJSON can marshal in two forms: just an id string,
// or an object containing a full data model
func (d *Dataset) UnmarshalJSON(data []byte) error {
	ds := _dataset{}
	if err := json.Unmarshal(data, &ds); err != nil {
		return err
	}

	*d = Dataset(ds)
	if err := d.ValidDataSource(); err != nil {
		return err
	}

	errs := AddressErrors(d, &[]Address{})
	if len(errs) > 0 {
		return errs[0]
	}

	return nil
}

func (ds *Dataset) ValidDataSource() error {
	if count := truthCount(ds.Url != "", ds.File != "", len(ds.Data) > 0); count > 1 {
		return errors.New("only one of url, file, or data can be set")
	} else if count == 1 {
		if ds.Format == UnknownDataFormat {
			// if format is unspecified, we need to be able to derive the format from
			// the extension of either the url or filepath
			if ds.DataFormat() == "" {
				return errors.New("format is required for data source")
			}
		}
	}

	return nil
}

func (ds *Dataset) RowToStrings(row []interface{}) (strs []string, err error) {
	if len(row) != len(ds.Fields) {
		err = fmt.Errorf("row is not the same length as the dataset's fields")
		return
	}
	strs = make([]string, len(ds.Fields))
	for i, field := range ds.Fields {
		str, err := field.Type.ValueToString(row[i])
		if err != nil {
			return nil, err
		}
		strs[i] = str
	}
	return
}

func (ds *Dataset) RowToBytes(row []interface{}) (bytes [][]byte, err error) {
	if len(row) != len(ds.Fields) {
		err = fmt.Errorf("row is not the same length as the dataset's fields")
		return
	}
	bytes = make([][]byte, len(ds.Fields))
	for i, field := range ds.Fields {
		val, err := field.Type.ValueToBytes(row[i])
		if err != nil {
			return nil, err
		}
		bytes[i] = val
	}
	return
}

type WalkDatasetsFunc func(int, *Dataset) error

func (ds *Dataset) WalkDatasets(depth int, fn WalkDatasetsFunc) (err error) {
	// call once for base dataset
	if err = fn(depth, ds); err != nil {
		return
	}

	depth++
	for _, d := range ds.Datasets {
		if err = d.WalkDatasets(depth, fn); err != nil {
			return
		}
	}

	return
}

func (ds *Dataset) DatasetForAddress(a Address) (match *Dataset, err error) {
	err = ds.WalkDatasets(0, func(depth int, d *Dataset) error {
		if a.Equal(d.Address) {
			match = d
			return errors.New("EOF")
		}
		return nil
	})

	if err != nil && err.Error() == "EOF" {
		return match, nil
	}

	return nil, ErrNotFound
}

type DataIteratorFunc func(int, [][]byte, error) error

func (ds *Dataset) EachRow(fn DataIteratorFunc) error {
	switch ds.dataFormat() {
	case CsvDataFormat:
		r := csv.NewReader(bytes.NewReader(ds.Data))
		num := 1
		for {
			csvRec, err := r.Read()
			if err != nil {
				if err.Error() == "EOF" {
					return nil
				}
				return err
			}

			rec := make([][]byte, len(csvRec))
			for i, col := range csvRec {
				rec[i] = []byte(col)
			}

			if err := fn(num, rec, err); err != nil {
				if err.Error() == "EOF" {
					return nil
				}
				return err
			}
			num++
		}
		// case dataset.JsonDataFormat:
	}

	return fmt.Errorf("cannot parse data format '%s'", ds.dataFormat())
}

// Ugh, should this exist?
func (d *Dataset) AllRows() (data [][][]byte, err error) {
	err = d.EachRow(func(_ int, row [][]byte, e error) error {
		if e != nil {
			return e
		}
		data = append(data, row)
		return nil
	})

	return
}

// TODO - need to resolve weather "DataFormat" enum needs to exist...
// and then decide on one of these method signatures
func (d *Dataset) dataFormat() DataFormat {
	if d.Format != UnknownDataFormat {
		return d.Format
	}

	if d.File != "" {
		f, _ := ParseDataFormatString(fs.PathExt(d.File))
		return f
	}

	if d.Url != "" {
		f, _ := ParseDataFormatString(fs.PathExt(d.Url))
		return f
	}

	return UnknownDataFormat
}

func (d *Dataset) DataFormat() string {
	if d.Format != UnknownDataFormat {
		return d.Format.String()
	}

	if d.File != "" {
		return fs.PathExt(d.File)
	}

	if d.Url != "" {
		return fs.PathExt(d.Url)
	}

	return ""
}
