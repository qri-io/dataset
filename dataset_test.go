package dataset

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestDatasetUnmarshalJSON(t *testing.T) {
	cases := []struct {
		FileName string
		result   *Dataset
		err      error
	}{
		{"airport-codes.json", AirportCodes, nil},
		{"continent-codes.json", ContinentCodes, nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(filepath.Join("test_data/definitions", c.FileName))
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Dataset{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d parse error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err = DatasetEqual(ds, c.result); err != nil {
			t.Errorf("case %d dataset comparison error: %s", i, err)
			continue
		}

	}
}

func DatasetEqual(a, b *Dataset) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("Dataset mismatch: %s != %s", a, b)
	}

	if !a.Address.Equal(b.Address) {
		return fmt.Errorf("address mismatch: %s != %s", a.Address, b.Address)
	}

	if a.Name != b.Name {
		return fmt.Errorf("Name mismatch: %s != %s", a.Name, b.Name)
	}
	if !a.Address.Equal(b.Address) {
		return fmt.Errorf("Address mismatch: %s != %s", a.Address, b.Address)
	}
	if a.Url != b.Url {
		return fmt.Errorf("Url mismatch: %s != %s", a.Url, b.Url)
	}
	if a.File != b.File {
		return fmt.Errorf("File mismatch: %s != %s", a.File, b.File)
	}
	// if a.Data != b.Data {
	// 	return fmt.Errorf("Data mismatch: %s != %s", a.Data, b.Data)
	// }
	if a.Format != b.Format {
		return fmt.Errorf("Format mismatch: %s != %s", a.Format, b.Format)
	}
	if err := CompareFormatOptions(a.FormatOptions, b.FormatOptions); err != nil {
		// return fmt.Errorf("FormatOptions mismatch: %s != %s", a.FormatOptions, b.FormatOptions)
		return err
	}
	// if a.Fields != b.Fields {
	// 	return fmt.Errorf("Fields mismatch: %s != %s", a.Fields, b.Fields)
	// }
	// if a.PrimaryKey != b.PrimaryKey {
	// 	return fmt.Errorf("PrimaryKey mismatch: %s != %s", a.PrimaryKey, b.PrimaryKey)
	// }
	if a.Query != b.Query {
		return fmt.Errorf("Query mismatch: %s != %s", a.Query, b.Query)
	}
	if a.Mediatype != b.Mediatype {
		return fmt.Errorf("Mediatype mismatch: %s != %s", a.Mediatype, b.Mediatype)
	}
	if a.Encoding != b.Encoding {
		return fmt.Errorf("Encoding mismatch: %s != %s", a.Encoding, b.Encoding)
	}
	if a.Bytes != b.Bytes {
		return fmt.Errorf("Bytes mismatch: %s != %s", a.Bytes, b.Bytes)
	}
	if a.Hash != b.Hash {
		return fmt.Errorf("Hash mismatch: %s != %s", a.Hash, b.Hash)
	}
	// if a.Datasets != b.Datasets {
	// 	return fmt.Errorf("Datasets mismatch: %s != %s", a.Datasets, b.Datasets)
	// }
	if a.Readme != b.Readme {
		return fmt.Errorf("Readme mismatch: %s != %s", a.Readme, b.Readme)
	}
	if a.Author != b.Author {
		return fmt.Errorf("Author mismatch: %s != %s", a.Author, b.Author)
	}
	if a.Image != b.Image {
		return fmt.Errorf("Image mismatch: %s != %s", a.Image, b.Image)
	}
	if a.Description != b.Description {
		return fmt.Errorf("Description mismatch: %s != %s", a.Description, b.Description)
	}
	if a.Homepage != b.Homepage {
		return fmt.Errorf("Homepage mismatch: %s != %s", a.Homepage, b.Homepage)
	}
	if a.IconImage != b.IconImage {
		return fmt.Errorf("IconImage mismatch: %s != %s", a.IconImage, b.IconImage)
	}
	if a.PosterImage != b.PosterImage {
		return fmt.Errorf("PosterImage mismatch: %s != %s", a.PosterImage, b.PosterImage)
	}
	if err := CompareLicense(a.License, b.License); err != nil {
		return err
	}
	if a.Version != b.Version {
		return fmt.Errorf("Version mismatch: %s != %s", a.Version, b.Version)
	}
	// if a.Keywords != b.Keywords {
	// 	return fmt.Errorf("Keywords mismatch: %s != %s", a.Keywords, b.Keywords)
	// }
	// if a.Contributors != b.Contributors {
	// 	return fmt.Errorf("Contributors mismatch: %s != %s", a.Contributors, b.Contributors)
	// }
	// if a.Sources != b.Sources {
	// 	return fmt.Errorf("Sources mismatch: %s != %s", a.Sources, b.Sources)
	// }

	return nil
}

func CompareLicense(a, b *License) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("License mistmatch: %s != %s", a, b)
	}

	if a.Type != b.Type {
		return fmt.Errorf("type mismatch: '%s' != '%s'", a.Type, b.Type)
	}

	return nil
}

func CompareFormatOptions(a, b FormatOptions) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("FormatOptions mismatch: %s != %s", a, b)
	}

	if a.Format() != b.Format() {
		return fmt.Errorf("FormatOptions mistmatch %s != %s", a.Format(), b.Format())
	}
	// TODO - exhaustive check

	return nil
}
