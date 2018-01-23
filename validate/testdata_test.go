package validate

import (
	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

var emptyRawText = ``

// has lazy quotes
var rawText1 = `first_name,last_name,username,age
"Rob","Pike",rob, 100
Ken,Thompson,ken, 75.5
"Robert","Griesemer","gri", 100`

var namesStructure = &dataset.Structure{
	Format: dataset.CSVDataFormat,
	FormatConfig: &dataset.CSVOptions{
		HeaderRow: true,
	},
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": {
			"type": "array",
			"items": [
				{"title": "first_name", "type": "string" },
				{"title": "last_name", "type": "string" },
				{"title": "username", "type": "string" },
				{"title": "age", "type": "integer" }
			]
		}
	}`),
}

// has nonNumeric quotes and comma inside quotes on last line
var rawText2 = `"first_name","last_name","username","age"
"Rob","Pike","rob", 22
"Robert","Griesemer","gri", 100
"abc","def,ghi","jkl",1000`

// same as above but with spaces in last line
var rawText2b = `"first_name","last_name","username","age"
"Rob","Pike","rob", 22
"Robert","Griesemer","gri", 100
"abc", "def,ghi", "jkl", 1000`

// error in last row "age" column
var rawText2c = `first_name,last_name,username,age
"Rob","Pike","rob",22
"Robert","Griesemer","gri",100
"abc","def,ghi","jkl",_`

// NOTE: technically this is valid csv and we should be catching this at an earlier filter
var rawText3 = `<html>
<body>
<table>
<th>
<tr>col</tr>
</th>
</table>
</body>
</html>`

var rawText4 = `<html>
<body>
<table>
<th>
<tr>Last Name, First</tr>
<tr>
</th>
</table>
</body>
</html>`

var cdxjStructure = &dataset.Structure{
	Format: dataset.CDXJDataFormat,
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": {
			"type": "array",
			"items": [
				{"title": "url", "type": "string"},
				{"title": "date", "type": "string"},
				{"title": "record_type", "type": "string"},
				{"title": "meta", "type": "object"}
			]
		}
	}`),
}

const cdxjRawText = `!OpenWayback-CDXJ 1.0
(com,cnn,)/world> 2015-09-03T13:27:52Z response {"a":0,"b":"b","c":false}
(uk,ac,rpms,)/> 2015-09-03T13:27:52Z request {"frequency":241,"spread":3}
(uk,co,bbc,)/images> 2015-09-03T13:27:52Z response {"frequency":725,"spread":1}
`
