package dataset

import (
	"github.com/qri-io/dataset/datatypes"
)

var AirportCodes = &Dataset{
	Kind:     KindDataset,
	Title:    "Airport Codes",
	Homepage: "http://www.ourairports.com/",
	License: &License{
		Type: "PDDL-1.0",
	},
	Citations: []*Citation{
		{
			Name: "Our Airports",
			URL:  "http://ourairports.com/data/",
		},
	},
	Commit:    &CommitMsg{Title: "initial commit"},
	Structure: AirportCodesStructure,
	// File:   "data/airport-codes.csv",
	// Readme: "readme.md",
	// Format: "text/csv",
}

var AirportCodesAbstract = &Dataset{
	Kind:      KindDataset,
	Structure: AirportCodesStructureAbstract,
}

const AirportCodesJSON = `{"citations":[{"name":"Our Airports","url":"http://ourairports.com/data/"}],"commit":{"title":"initial commit"},"homepage":"http://www.ourairports.com/","kind":"qri:ds:0","license":"PDDL-1.0","structure":{"format":"csv","formatConfig":{"headerRow":true},"kind":"qri:st:0","schema":{"fields":[{"name":"ident","type":"string"},{"name":"type","type":"string"},{"name":"name","type":"string"},{"name":"latitude_deg","type":"float"},{"name":"longitude_deg","type":"float"},{"name":"elevation_ft","type":"integer"},{"name":"continent","type":"string"},{"name":"iso_country","type":"string"},{"name":"iso_region","type":"string"},{"name":"municipality","type":"string"},{"name":"gps_code","type":"string"},{"name":"iata_code","type":"string"},{"name":"local_code","type":"string"}]}},"title":"Airport Codes"}`

var AirportCodesStructure = &Structure{
	Format: CSVDataFormat,
	Kind:   KindStructure,
	FormatConfig: &CSVOptions{
		HeaderRow: true,
	},
	Schema: &Schema{
		Fields: []*Field{
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

var AirportCodesStructureAbstract = &Structure{
	Format:       CSVDataFormat,
	FormatConfig: &CSVOptions{HeaderRow: true},
	Schema: &Schema{
		Fields: []*Field{
			{
				Name: "a",
				Type: datatypes.String,
			},
			{
				Name: "b",
				Type: datatypes.String,
			},
			{
				Name: "c",
				Type: datatypes.String,
			},
			{
				Name: "d",
				Type: datatypes.Float,
			},
			{
				Name: "e",
				Type: datatypes.Float,
			},
			{
				Name: "f",
				Type: datatypes.Integer,
			},
			{
				Name: "g",
				Type: datatypes.String,
			},
			{
				Name: "h",
				Type: datatypes.String,
			},
			{
				Name: "i",
				Type: datatypes.String,
			},
			{
				Name: "j",
				Type: datatypes.String,
			},
			{
				Name: "k",
				Type: datatypes.String,
			},
			{
				Name: "l",
				Type: datatypes.String,
			},
			{
				Name: "m",
				Type: datatypes.String,
			},
		},
	},
}

var ContinentCodes = &Dataset{
	Title:       "Continent Codes",
	Kind:        KindDataset,
	Description: "list of continents with corresponding two letter codes",
	License: &License{
		Type: "odc-pddl",
		URL:  "http://opendatacommons.org/licenses/pddl/",
	},
	Keywords: []string{
		"Continents",
		"Two letter code",
		"Continent codes",
		"Continent code list",
	},
}

var ContinentCodesStructure = &Structure{
	Format: CSVDataFormat,
	Schema: &Schema{
		Fields: []*Field{
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

var Hours = &Dataset{
	Title: "hours",
	Kind:  KindDataset,
	Data:  "/ipfs/QmS1dVa1xemo7gQzJgjimj1WwnVBF3TwRTGsyKa1uEBWbJ",
}

var HoursStructure = &Structure{
	Format: CSVDataFormat,
	Schema: &Schema{
		Fields: []*Field{
			{Name: "field_1", Type: datatypes.Date},
			{Name: "field_2", Type: datatypes.Float},
			{Name: "field_3", Type: datatypes.String},
			{Name: "field_4", Type: datatypes.String},
		},
	},
}
