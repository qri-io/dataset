package dsfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

var AirportCodes = &dataset.Dataset{
	Meta: &dataset.Meta{
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
	Qri:     dataset.KindCommit,
	Message: "initial commit",
}

var AirportCodesStructure = &dataset.Structure{
	Format: dataset.CSVDataFormat,
	FormatConfig: &dataset.CSVOptions{
		HeaderRow: true,
	},
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": {
			"type" : "array",
			"items" : [
				{"title": "ident", "type": "string" },
				{"title": "type", "type": "string" },
				{"title": "name", "type": "string" },
				{"title": "latitude_deg", "type": "number" },
				{"title": "longitude_deg", "type": "number" },
				{"title": "elevation_ft", "type": "integer" },
				{"title": "continent", "type": "string" },
				{"title": "iso_country", "type": "string" },
				{"title": "iso_region", "type": "string" },
				{"title": "municipality", "type": "string" },
				{"title": "gps_code", "type": "string" },
				{"title": "iata_code", "type": "string" },
				{"title": "local_code", "type": "string" }
			]
		}
	}`),
}

var AirportCodesStructureAgebraic = &dataset.Structure{
	Format:       dataset.CSVDataFormat,
	FormatConfig: &dataset.CSVOptions{HeaderRow: true},
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": {
			"type": "array",
			"items": [
				{"title": "col_0", "type": "string" },
				{"title": "col_1", "type": "string" },
				{"title": "col_2", "type": "string" },
				{"title": "col_3", "type": "number" },
				{"title": "col_4", "type": "number" },
				{"title": "col_5", "type": "integer" },
				{"title": "col_6", "type": "string" },
				{"title": "col_7", "type": "string" },
				{"title": "col_8", "type": "string" },
				{"title": "col_9", "type": "string" },
				{"title": "col_10", "type": "string" },
				{"title": "col_11", "type": "string" },
				{"title": "col_12", "type": "string" }
			]
		}
		}`),
}

var ContinentCodes = &dataset.Dataset{
	Qri: dataset.KindDataset,
	Meta: &dataset.Meta{
		Qri:         dataset.KindMeta,
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
	Schema: jsonschema.Must(`{
		"type": "array",
		"items" : {
			"type": "array",
			"items" : [
				{"title": "code", "type": "string"},
				{"title": "name", "type": "string"}
			]
		} 
	}`),
}

var Hours = &dataset.Dataset{
	Meta: &dataset.Meta{
		Title: "hours",
	},
	// Data:   datastore.NewKey("/ipfs/QmS1dVa1xemo7gQzJgjimj1WwnVBF3TwRTGsyKa1uEBWbJ"),
}

var HoursStructure = &dataset.Structure{
	Format: dataset.CSVDataFormat,
	Schema: jsonschema.Must(`{
		"type":"array",
		"items": {
			"type": "array",
			"items": [
				{"title": "field_1", "type": "string" },
				{"title": "field_2", "type": "number" },
				{"title": "field_3", "type": "string" },
				{"title": "field_4", "type": "string" }
			]
		}
	}`),
}

func makeFilestore() (map[string]datastore.Key, cafs.Filestore, error) {
	fs := cafs.NewMapstore()

	datasets := map[string]datastore.Key{
		"movies": datastore.NewKey(""),
		"cities": datastore.NewKey(""),
	}

	for k := range datasets {
		dsdata, err := ioutil.ReadFile(fmt.Sprintf("testdata/%s/input.dataset.json", k))
		if err != nil {
			return datasets, nil, err
		}

		ds := &dataset.Dataset{}
		if err := json.Unmarshal(dsdata, ds); err != nil {
			return datasets, nil, err
		}

		dataPath := fmt.Sprintf("testdata/%s/data.%s", k, ds.Structure.Format.String())
		data, err := ioutil.ReadFile(dataPath)
		if err != nil {
			return datasets, nil, err
		}

		df := cafs.NewMemfileBytes(filepath.Base(dataPath), data)

		dskey, err := WriteDataset(fs, ds, df, true)
		if err != nil {
			return datasets, nil, fmt.Errorf("dataset: %s write error: %s", k, err.Error())
		}
		datasets[k] = dskey
	}

	return datasets, fs, nil
}
