package detect

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qri-io/dataset"
)

var egCorruptCsvData = []byte(`
		"""fhkajslfnakjlcdnajcl ashklj asdhcjklads ch,,,\dagfd
	`)

var egNaicsCsvData = []byte(`
STATE,FIRM,PAYR_N,PAYRFL_N,STATEDSCR,NAICSDSCR,entrsizedscr
00,--,74883.53,5621697325,United States,Total,01:  Total
00,--,35806.37,241347624,United States,Total,02:  0-4`)

var egNoHeaderData1 = []byte(`
example,false,other,stuff
ex,true,text,col
		`)

var egNoHeaderData2 = []byte(`
this,example,has,a,number,column,1
this,example,has,a,number,column,2
this,example,has,a,number,column,3`)

var egNoHeaderData3 = []byte(`
one, 1, three
one, 2, three`)

var egNoHeaderData4 = []byte(`one,two,3
four,five,6`)

var egNonDeterministicHeader = []byte(`
not,possible,to,tell,if,this,csv,data,has,a,header
not,possible,to,tell,if,this,csv,data,has,a,header
not,possible,to,tell,if,this,csv,data,has,a,header
not,possible,to,tell,if,this,csv,data,has,a,header
`)

func TestDetermineCSVSchema(t *testing.T) {

	runTestCase(t, "noHeaderData1", egNoHeaderData1,
		map[string]interface{}{
			"items": map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"title": "field_1",
						"type":  "string",
					},
					map[string]interface{}{
						"title": "field_2",
						"type":  "boolean",
					}, map[string]interface{}{
						"title": "field_3",
						"type":  "string",
					}, map[string]interface{}{
						"title": "field_4",
						"type":  "string",
					},
				},
				"type": "array",
			},
			"type": "array",
		})

	runTestCase(t, "noHeaderData2", egNoHeaderData2,
		map[string]interface{}{
			"items": map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"title": "field_1",
						"type":  "string",
					},
					map[string]interface{}{
						"title": "field_2",
						"type":  "string",
					}, map[string]interface{}{
						"title": "field_3",
						"type":  "string",
					}, map[string]interface{}{
						"title": "field_4",
						"type":  "string",
					}, map[string]interface{}{
						"title": "field_5",
						"type":  "string",
					}, map[string]interface{}{
						"title": "field_6",
						"type":  "string",
					}, map[string]interface{}{
						"title": "field_7",
						"type":  "integer",
					},
				},
				"type": "array",
			},
			"type": "array",
		})

	runTestCase(t, "noHeaderData3", egNoHeaderData3,
		map[string]interface{}{
			"items": map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"title": "field_1",
						"type":  "string",
					},
					map[string]interface{}{
						"title": "field_2",
						"type":  "integer",
					}, map[string]interface{}{
						"title": "field_3",
						"type":  "string",
					},
				},
				"type": "array",
			},
			"type": "array",
		})

	runTestCase(t, "noHeaderData4", egNoHeaderData4,
		map[string]interface{}{
			"items": map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{
						"title": "field_1",
						"type":  "string",
					},
					map[string]interface{}{
						"title": "field_2",
						"type":  "string",
					}, map[string]interface{}{
						"title": "field_3",
						"type":  "integer",
					},
				},
				"type": "array",
			},
			"type": "array",
		})
}

func runTestCase(t *testing.T, description string, input []byte, expect map[string]interface{}) {
	st := dataset.Structure{Format: "csv"}
	reader := bytes.NewReader(input)
	schema, _, err := CSVSchema(&st, reader)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(expect, schema); diff != "" {
		t.Errorf("mismatch for \"%s\" (-want +got):\n%s\n", description, diff)
	}
}
