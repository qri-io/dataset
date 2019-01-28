package dataset

import (
	"github.com/qri-io/jsonschema"
)

var AirportCodes = &Dataset{
	Qri: KindDataset.String(),
	Meta: &Meta{
		Qri:     KindMeta.String(),
		Title:   "Airport Codes",
		HomeURL: "http://www.ourairports.com/",
		License: &License{
			Type: "PDDL-1.0",
		},
		Citations: []*Citation{
			{
				Name: "Our Airports",
				URL:  "http://ourairports.com/data/",
			},
		},
	},
	Commit:    &Commit{Title: "initial commit"},
	Structure: AirportCodesStructure,
}

var AirportCodesAbstract = &Dataset{
	Qri:       KindDataset.String(),
	Structure: AirportCodesStructureAbstract,
}

const AirportCodesJSON = `{"commit":{"qri":"cm:0","timestamp":"0001-01-01T00:00:00Z","title":"initial commit"},"meta":{"citations":[{"name":"Our Airports","url":"http://ourairports.com/data/"}],"homeURL":"http://www.ourairports.com/","license":{"type":"PDDL-1.0"},"qri":"md:0","title":"Airport Codes"},"qri":"ds:0","structure":{"errCount":5,"format":"csv","formatConfig":{"headerRow":true},"qri":"st:0","schema":{"items":{"items":[{"title":"ident","type":"string"},{"title":"type","type":"string"},{"title":"name","type":"string"},{"title":"latitude_deg","type":"string"},{"title":"longitude_deg","type":"string"},{"title":"elevation_ft","type":"string"},{"title":"continent","type":"string"},{"title":"iso_country","type":"string"},{"title":"iso_region","type":"string"},{"title":"municipality","type":"string"},{"title":"gps_code","type":"string"},{"title":"iata_code","type":"string"},{"title":"local_code","type":"string"}],"type":"array"},"type":"array"}}}`

var AirportCodesStructure = &Structure{
	ErrCount: 5,
	Format:   CSVDataFormat,
	Qri:      KindStructure.String(),
	// FormatConfig: &CSVOptions{
	// 	HeaderRow: true,
	// },
	FormatConfig: map[string]interface{}{
		"headerRow": true,
	},
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": {
			"type": "array",
			"items": [
				{ "title": "ident", "type": "string"},
				{ "title": "type", "type": "string"},
				{ "title": "name", "type": "string"},
				{ "title": "latitude_deg", "type": "string"},
				{ "title": "longitude_deg", "type": "string"},
				{ "title": "elevation_ft", "type": "string"},
				{ "title": "continent", "type": "string"},
				{ "title": "iso_country", "type": "string"},
				{ "title": "iso_region", "type": "string"},
				{ "title": "municipality", "type": "string"},
				{ "title": "gps_code", "type": "string"},
				{ "title": "iata_code", "type": "string"},
				{ "title": "local_code", "type": "string"}
			]
		}
	}`),
}

var AirportCodesStructureAbstract = &Structure{
	Format: CSVDataFormat,
	// FormatConfig: &CSVOptions{HeaderRow: true},
	FormatConfig: map[string]interface{}{
		"headerRow": true,
	},
	// Schema: jsonschema.Must(`{
	// 	"type": "array",
	// 	"items": {
	// 		"type": "array",
	// 		"items": [
	// 			{ "title": "a", "type": "string"},
	// 			{ "title": "b", "type": "string"},
	// 			{ "title": "c", "type": "string"},
	// 			{ "title": "d", "type": "number"},
	// 			{ "title": "e", "type": "number"},
	// 			{ "title": "f", "type": "integer"},
	// 			{ "title": "g", "type": "string"},
	// 			{ "title": "h", "type": "string"},
	// 			{ "title": "i", "type": "string"},
	// 			{ "title": "j", "type": "string"},
	// 			{ "title": "k", "type": "string"},
	// 			{ "title": "l", "type": "string"},
	// 			{ "title": "m", "type": "string"}
	// 		]
	// 	}
	// }`),
}

var ContinentCodes = &Dataset{
	Qri: KindDataset.String(),
	Meta: &Meta{
		Title:       "Continent Codes",
		Qri:         KindMeta.String(),
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
	},
}

var ContinentCodesStructure = &Structure{
	Format: CSVDataFormat,
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": [
			{"title": "code", "type": "string"},
			{"title": "name", "type": "string"}
		]
	}`),
}

var Hours = &Dataset{
	Qri: KindDataset.String(),
	Meta: &Meta{
		Title:       "hours",
		Qri:         KindMeta.String(),
		AccessURL:   "https://example.com/not/a/url",
		DownloadURL: "https://example.com/not/a/url",
		ReadmeURL:   "/ipfs/notahash",
	},
	BodyPath: "/ipfs/QmS1dVa1xemo7gQzJgjimj1WwnVBF3TwRTGsyKa1uEBWbJ",
}

var HoursStructure = &Structure{
	Format: CSVDataFormat,
	Depth:  2,
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": {
			"type": "array",
			"items": [
				{ "title": "field_1", "type": "string"},
				{ "title": "field_2", "type": "number"},
				{ "title": "field_3", "type": "string"},
				{ "title": "field_4", "type": "string"}
			]
		}
	}`),
}
