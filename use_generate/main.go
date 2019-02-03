package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/generate"
)

func main() {
	generateExamples(dataset.BaseSchemaObject, 40, "data_obj_%d")
	generateExamples(dataset.BaseSchemaArray, 40, "data_array_%d")
}

func generateExamples(schema map[string]interface{}, numExamples int, template string) {
	sta := dataset.Structure{Format: "json", Schema: schema}
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
