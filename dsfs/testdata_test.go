package dsfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/datatypes"
)

var AirportCodes = &dataset.Dataset{
	Metadata: &dataset.Metadata{
		Title:    "Airport Codes",
		HomePath: "http://www.ourairports.com/",
		License: &dataset.License{
			Type: "PDDL-1.0",
		},
		Citations: []*dataset.Citation{
			{
				Name: "Our Airports",
				URL:  "http://ourairports.com/data/",
			},
		},
	},
	// File:   "data/airport-codes.csv",
	// Readme: "readme.md",
	// Format: "text/csv",
}

var AirportCodesCommit = &dataset.Commit{
	Kind:    dataset.KindCommit,
	Message: "initial commit",
}

var AirportCodesStructure = &dataset.Structure{
	Format: dataset.CSVDataFormat,
	FormatConfig: &dataset.CSVOptions{
		HeaderRow: true,
	},
	Schema: &dataset.Schema{
		Fields: []*dataset.Field{
			{
				Name: "ident",
				Type: datatypes.String,
			},
			{
				Name: "type",
				Type: datatypes.String,
			},
			{
				Name: "name",
				Type: datatypes.String,
			},
			{
				Name: "latitude_deg",
				Type: datatypes.Float,
			},
			{
				Name: "longitude_deg",
				Type: datatypes.Float,
			},
			{
				Name: "elevation_ft",
				Type: datatypes.Integer,
			},
			{
				Name: "continent",
				Type: datatypes.String,
			},
			{
				Name: "iso_country",
				Type: datatypes.String,
			},
			{
				Name: "iso_region",
				Type: datatypes.String,
			},
			{
				Name: "municipality",
				Type: datatypes.String,
			},
			{
				Name: "gps_code",
				Type: datatypes.String,
			},
			{
				Name: "iata_code",
				Type: datatypes.String,
			},
			{
				Name: "local_code",
				Type: datatypes.String,
			},
		},
	},
}

var AirportCodesStructureAgebraic = &dataset.Structure{
	Format:       dataset.CSVDataFormat,
	FormatConfig: &dataset.CSVOptions{HeaderRow: true},
	Schema: &dataset.Schema{
		Fields: []*dataset.Field{
			{
				Name: "col_0",
				Type: datatypes.String,
			},
			{
				Name: "col_1",
				Type: datatypes.String,
			},
			{
				Name: "col_2",
				Type: datatypes.String,
			},
			{
				Name: "col_3",
				Type: datatypes.Float,
			},
			{
				Name: "col_4",
				Type: datatypes.Float,
			},
			{
				Name: "col_5",
				Type: datatypes.Integer,
			},
			{
				Name: "col_6",
				Type: datatypes.String,
			},
			{
				Name: "col_7",
				Type: datatypes.String,
			},
			{
				Name: "col_8",
				Type: datatypes.String,
			},
			{
				Name: "col_9",
				Type: datatypes.String,
			},
			{
				Name: "col_10",
				Type: datatypes.String,
			},
			{
				Name: "col_11",
				Type: datatypes.String,
			},
			{
				Name: "col_12",
				Type: datatypes.String,
			},
		},
	},
}

var ContinentCodes = &dataset.Dataset{
	Kind: dataset.KindDataset,
	Metadata: &dataset.Metadata{
		Kind:        dataset.KindMetadata,
		Title:       "Continent Codes",
		Description: "list of continents with corresponding two letter codes",
		License: &dataset.License{
			Type: "odc-pddl",
			URL:  "http://opendatacommons.org/licenses/pddl/",
		},
		Keywords: []string{
			"Continents",
			"Two letter code",
			"Continent codes",
			"Continent code list",
		},
	},
}

var ContinentCodesStructure = &dataset.Structure{
	Format: dataset.CSVDataFormat,
	Schema: &dataset.Schema{
		Fields: []*dataset.Field{
			{
				Name: "Code",
				Type: datatypes.String,
			},
			{
				Name: "Name",
				Type: datatypes.String,
			},
		},
	},
}

var Hours = &dataset.Dataset{
	Metadata: &dataset.Metadata{
		Title: "hours",
	},
	// Data:   datastore.NewKey("/ipfs/QmS1dVa1xemo7gQzJgjimj1WwnVBF3TwRTGsyKa1uEBWbJ"),
}

var HoursStructure = &dataset.Structure{
	Format: dataset.CSVDataFormat,
	Schema: &dataset.Schema{
		Fields: []*dataset.Field{
			{Name: "field_1", Type: datatypes.Date},
			{Name: "field_2", Type: datatypes.Float},
			{Name: "field_3", Type: datatypes.String},
			{Name: "field_4", Type: datatypes.String},
		},
	},
}

func makeFilestore() (map[string]datastore.Key, cafs.Filestore, error) {
	fs := memfs.NewMapstore()

	datasets := map[string]datastore.Key{
		"movies":  datastore.NewKey(""),
		"cities":  datastore.NewKey(""),
		"archive": datastore.NewKey(""),
	}

	for k := range datasets {
		dsdata, err := ioutil.ReadFile(fmt.Sprintf("testdata/%s.json", k))
		if err != nil {
			return datasets, nil, err
		}

		ds := &dataset.Dataset{}
		if err := json.Unmarshal(dsdata, ds); err != nil {
			return datasets, nil, err
		}

		rawdata, err := ioutil.ReadFile(fmt.Sprintf("testdata/%s.%s", k, ds.Structure.Format.String()))
		if err != nil {
			return datasets, nil, err
		}

		datakey, err := fs.Put(memfs.NewMemfileBytes(k, rawdata), true)
		if err != nil {
			return datasets, nil, err
		}

		ds.DataPath = datakey.String()
		dskey, err := SaveDataset(fs, ds, true)
		if err != nil {
			return datasets, nil, err
		}
		datasets[k] = dskey
	}

	return datasets, fs, nil
}
