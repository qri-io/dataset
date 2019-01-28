package dsio

import (
	"bytes"
	"fmt"

	"github.com/qri-io/dataset"
)

// Fuzz is the entry-point for go-fuzz. Return 1 for a successful parse and 0 for failures.
func Fuzz(data []byte) int {
	r := bytes.NewReader(data)
	st := &dataset.Structure{Format: dataset.JSONDataFormat.String(), Schema: dataset.BaseSchemaObject}
	reader, err := NewJSONReader(st, r)
	if err != nil {
		return 0
	}
	for {
		_, err = reader.ReadEntry()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("Error: %s\n", err.Error())
			return 0
		}
	}
	return 1
}
