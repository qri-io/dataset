package dataset

import (
	"bytes"
	"encoding/csv"
	"net/http"
	"net/url"

	"github.com/qri-io/datatype"
	"github.com/qri-io/fs"

	"fmt"
)

func AddressErrors(a *Dataset, prev *[]Address) (errs []error) {
	if a.Address == nil || a.Address.IsEmpty() {
		errs = append(errs, fmt.Errorf("address cannot be empty"))
		return
	}

	if err := checkDup(a.Address, prev); err != nil {
		errs = append(errs, err)
	}

	// query datasets get to skip ancestry validation
	if a.Query == nil {
		for _, ds := range a.Datasets {
			if err := checkDup(ds.Address, prev); err != nil {
				errs = append(errs, err)
			} else {
				if !a.Address.IsAncestor(ds.Address) {
					errs = append(errs, fmt.Errorf("%s cannot be a child of %s", ds.Address.String(), a.Address.String()))
				} else if a.Address.Equal(ds.Address) {
					errs = append(errs, fmt.Errorf("%s cannot be a child of %s", ds.Address.String(), a.Address.String()))
				}
			}

			if ds.Datasets != nil {
				errs = append(errs, AddressErrors(ds, prev)...)
			}
		}
	}

	return
}

func checkDup(adr Address, prev *[]Address) error {
	for _, p := range *prev {
		if adr.Equal(p) {
			return fmt.Errorf("duplicate address: %s", adr)
		}
	}
	*prev = append(*prev, adr)
	return nil
}

type ErrFormat int

const (
	ErrFmtUnknown ErrFormat = iota
	ErrFmtOneHotMatrix
	ErrFmtErrStrings
)

type ValidateDataOpt struct {
	ErrorFormat ErrFormat
	DataFormat  DataFormat
}

func (ds *Dataset) ValidateData(store fs.Store, options ...func(*ValidateDataOpt)) (validation *Dataset, data []byte, count int, err error) {

	validation = &Dataset{
		Address: NewAddress(ds.Address.String(), "errors"),
		Format:  CsvDataFormat,
	}
	for _, f := range ds.Fields {
		validation.Fields = append(validation.Fields, &Field{Name: f.Name + "_err", Type: datatype.Integer})
	}

	dsData, e := ds.FetchBytes(store)
	if e != nil {
		err = e
		return
	}
	ds.Data = dsData

	buf := &bytes.Buffer{}
	cw := csv.NewWriter(buf)

	err = ds.EachRow(func(num int, row [][]byte, err error) error {
		if err != nil {
			return err
		}

		errData, errNum, _ := validateRow(ds.Fields, num, row)
		// data = append(data, errData)
		count += errNum

		csvRow := make([]string, len(errData))
		for i, d := range errData {
			csvRow[i] = string(d)
		}
		if err := cw.Write(csvRow); err != nil {
			fmt.Sprintln(err)
		}

		return nil
	})

	cw.Flush()
	data = buf.Bytes()

	return
}

func validateRow(fields []*Field, num int, row [][]byte) ([][]byte, int, error) {
	count := 0
	errors := make([][]byte, len(fields))
	if len(row) != len(fields) {
		return errors, count, fmt.Errorf("column mismatch. expected: %d, got: %d", len(fields), len(row))
	}

	for i, f := range fields {
		_, e := f.Type.Parse(row[i])
		if e != nil {
			count++
			errors[i] = []byte("1")
		} else {
			errors[i] = []byte("0")
		}
	}

	return errors, count, nil
}

func (ds *Dataset) ValidateDeadLinks(store fs.Store) (validation *Dataset, data []byte, count int, err error) {
	proj := map[int]int{}
	validation = &Dataset{
		Address: NewAddress(ds.Address.String(), "errors"),
		Format:  CsvDataFormat,
	}

	for i, f := range ds.Fields {
		if f.Type == datatype.Url {
			proj[i] = len(validation.Fields)
			validation.Fields = append(validation.Fields, f)
			validation.Fields = append(validation.Fields, &Field{Name: f.Name + "_dead", Type: datatype.Integer})
		}
	}

	dsData, e := ds.FetchBytes(store)
	if e != nil {
		err = e
		return
	}
	ds.Data = dsData

	buf := &bytes.Buffer{}
	cw := csv.NewWriter(buf)

	err = ds.EachRow(func(num int, row [][]byte, err error) error {
		if err != nil {
			return err
		}

		result := make([][]byte, len(validation.Fields))
		for l, r := range proj {
			result[r] = row[l]
			if err := checkUrl(string(result[r])); err != nil {
				count++
				result[r+1] = []byte("1")
			} else {
				result[r+1] = []byte("0")
			}
		}

		csvRow := make([]string, len(result))
		for i, d := range result {
			csvRow[i] = string(d)
		}
		if err := cw.Write(csvRow); err != nil {
			fmt.Sprintln(err)
		}

		return nil
	})

	cw.Flush()
	data = buf.Bytes()
	return
}

func checkUrl(rawurl string) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	res, err := http.Get(u.String())
	if err != nil {
		return err
	}
	res.Body.Close()
	fmt.Println(u.String(), res.StatusCode)
	if res.StatusCode > 399 {
		return fmt.Errorf("non-200 status code")
	}
	return nil
}
