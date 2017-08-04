package dataset

import (
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset/datatypes"
)

var AirportCodes = &Resource{
	// Name:     "airport-codes",
	// Address:  NewAddress("airport-codes"),
	// Homepage: "http://www.ourairports.com/",
	// Version:  "0.2.0",
	// License: &License{
	// 	Type: "PDDL-1.0",
	// },
	// Sources: []*Source{
	// 	&Source{
	// 		Name: "Our Airports",
	// 		Url:  "http://ourairports.com/data/",
	// 	},
	// },
	// File:   "data/airport-codes.csv",
	// Readme: "readme.md",
	// Format: "text/csv",
	Format: CsvDataFormat,
	FormatConfig: &CsvOptions{
		HeaderRow: true,
	},
	Schema: &Schema{
		Fields: []*Field{
			&Field{
				Name: "ident",
				Type: datatypes.String,
			},
			&Field{
				Name: "type",
				Type: datatypes.String,
			},
			&Field{
				Name: "name",
				Type: datatypes.String,
			},
			&Field{
				Name: "latitude_deg",
				Type: datatypes.Float,
			},
			&Field{
				Name: "longitude_deg",
				Type: datatypes.Float,
			},
			&Field{
				Name: "elevation_ft",
				Type: datatypes.Integer,
			},
			&Field{
				Name: "continent",
				Type: datatypes.String,
			},
			&Field{
				Name: "iso_country",
				Type: datatypes.String,
			},
			&Field{
				Name: "iso_region",
				Type: datatypes.String,
			},
			&Field{
				Name: "municipality",
				Type: datatypes.String,
			},
			&Field{
				Name: "gps_code",
				Type: datatypes.String,
			},
			&Field{
				Name: "iata_code",
				Type: datatypes.String,
			},
			&Field{
				Name: "local_code",
				Type: datatypes.String,
			},
		},
	},
}

var ContinentCodes = &Resource{
// Name:        "continent-codes",
// Address:     NewAddress("continent-codes"),
// Description: "Data contains list of continents and it's two letter codes",
// License: &License{
// 	Type: "odc-pddl",
// 	Url:  "http://opendatacommons.org/licenses/pddl/",
// },
// Keywords: []string{
// 	"Continents",
// 	"Two letter code",
// 	"Continent codes",
// 	"Continent code list",
// },

// "last_updated": "2016-03-25"
// Version: "0.1.0",
// Resources: []*Resource{
// 	&Resource{
// 		Address: NewAddress("continent-codes.2"),
// 		// "file": "data/continent-codes.csv",
// 		Description: "continent codes",
// 		Fields: []*Field{
// 			&Field{
// 				Name: "Code",
// 				Type: datatypes.String,
// 			},
// 			&Field{
// 				Name: "Name",
// 				Type: datatypes.String,
// 			},
// 		},
// 	},
// },
}

var Hours = &Resource{
	Format: CsvDataFormat,
	Path:   datastore.NewKey("/ipfs/QmS1dVa1xemo7gQzJgjimj1WwnVBF3TwRTGsyKa1uEBWbJ"),
	Schema: &Schema{
		Fields: []*Field{
			&Field{Name: "field_1", Type: datatypes.Date},
			&Field{Name: "field_2", Type: datatypes.Float},
			&Field{Name: "field_3", Type: datatypes.String},
			&Field{Name: "field_4", Type: datatypes.String},
		},
	},
}
