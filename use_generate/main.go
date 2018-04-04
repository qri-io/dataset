package main

import (
	"fmt"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/generate"
	"github.com/qri-io/jsonschema"
	"math/rand"
	"os"
)

func main() {
	generateExamples(dataset.BaseSchemaObject, 40, "data_obj_%d")
	generateExamples(dataset.BaseSchemaArray, 40, "data_array_%d")
}

func generateExamples(schema *jsonschema.RootSchema, numExamples int, template string) {
	sta := dataset.Structure{Format: dataset.JSONDataFormat, Schema: schema}
	g, _ := generate.NewGenerator(&sta, generate.AssignUseRandomType)
	for i := 0; i < numExamples; i++ {
		filename := fmt.Sprintf("out/"+template+".json", i)
		f, err := os.Create(filename)
		if err != nil {
			fmt.Printf("%s", err)
			return
		}
		writer, err := dsio.NewJSONWriter(&sta, f)
		if err != nil {
			fmt.Printf("%s", err)
			return
		}

		numEntries := rand.Intn(10)
		for j := 0; j < numEntries; j++ {
			ent, err := g.ReadEntry()
			if err != nil {
				fmt.Printf("%s", err)
				return
			}

			writer.WriteEntry(ent)
		}
		writer.Close()
	}
}
