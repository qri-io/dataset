package dataset

import (
	"github.com/qri-io/datatype"
)

var AirportCodes = &Dataset{
	Name:     "airport-codes",
	Address:  NewAddress("airport-codes"),
	Homepage: "http://www.ourairports.com/",
	Version:  "0.2.0",
	License: &License{
		Type: "PDDL-1.0",
	},
	Sources: []*Source{
		&Source{
			Name: "Our Airports",
			Url:  "http://ourairports.com/data/",
		},
	},
	File:      "data/airport-codes.csv",
	Format:    CsvDataFormat,
	Readme:    "readme.md",
	Mediatype: "text/csv",
	FormatOptions: &CsvOptions{
		HeaderRow: true,
	},
	Fields: []*Field{
		&Field{
			Name: "ident",
			Type: datatype.String,
		},
		&Field{
			Name: "type",
			Type: datatype.String,
		},
		&Field{
			Name: "name",
			Type: datatype.String,
		},
		&Field{
			Name: "latitude_deg",
			Type: datatype.Float,
		},
		&Field{
			Name: "longitude_deg",
			Type: datatype.Float,
		},
		&Field{
			Name: "elevation_ft",
			Type: datatype.Integer,
		},
		&Field{
			Name: "continent",
			Type: datatype.String,
		},
		&Field{
			Name: "iso_country",
			Type: datatype.String,
		},
		&Field{
			Name: "iso_region",
			Type: datatype.String,
		},
		&Field{
			Name: "municipality",
			Type: datatype.String,
		},
		&Field{
			Name: "gps_code",
			Type: datatype.String,
		},
		&Field{
			Name: "iata_code",
			Type: datatype.String,
		},
		&Field{
			Name: "local_code",
			Type: datatype.String,
		},
	},
}

var ContinentCodes = &Dataset{
	Name:        "continent-codes",
	Address:     NewAddress("continent-codes"),
	Description: "Data contains list of continents and it's two letter codes",
	License: &License{
		Type: "odc-pddl",
		Url:  "http://opendatacommons.org/licenses/pddl/",
	},
	Keywords: []string{
		"Continents",
		"Two letter code",
		"Continent codes",
		"Continent code list",
	},
	// "last_updated": "2016-03-25"
	Version: "0.1.0",
	Datasets: []*Dataset{
		&Dataset{
			Address: NewAddress("continent-codes.2"),
			// "file": "data/continent-codes.csv",
			Description: "continent codes",
			Fields: []*Field{
				&Field{
					Name: "Code",
					Type: datatype.String,
				},
				&Field{
					Name: "Name",
					Type: datatype.String,
				},
			},
		},
	},
}
