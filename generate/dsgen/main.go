package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/generate"
)

const help = `
dsgen generates random CSV data for given tabular structure & prints to stdout.
Use "fixed" to generate 1000byte rows for a fixed 4 column schema.

Usage:
	dsgen [structure.json] --rows [num_rows]
	dsgen fixed --rows [num_rows]
`

func main() {
	rowsPtr := flag.Int("rows", 1000, "number of entries (rows) to generate")

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Println(help)
		os.Exit(1)
	}
	if args[0] == "fixed" {
		if err := writeFixedFile(*rowsPtr, 0); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	} else {
		if err := generateFile(args[0], *rowsPtr); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

func generateFile(structurePath string, lines int) error {
	data, err := ioutil.ReadFile(structurePath)
	if err != nil {
		return err
	}
	st := &dataset.Structure{}
	if err := json.Unmarshal(data, st); err != nil {
		return err
	}

	gen, err := generate.NewTabularGenerator(st)
	if err != nil {
		return err
	}

	w, err := dsio.NewCSVWriter(st, os.Stdout)
	if err != nil {
		return err
	}

	for i := 0; i < lines; i++ {
		ent, err := gen.ReadEntry()
		if err != nil {
			return err
		}
		w.WriteEntry(ent)
	}
	w.Close()
	gen.Close()
	return nil
}

func writeFixedFile(lines, diffStart int) error {
	filler := strings.Repeat("0", 908)
	w := csv.NewWriter(os.Stdout)
	w.Write([]string{"uuid", "ingest", "occurred", "raw_data"})
	var uuid, ingest, occurred, rawData string
	for i := 0; i < lines; i++ {
		if diffStart > 0 && i > diffStart {
			// write a "diff" line
			uuid = fmt.Sprintf("%d-%d-BA882B47-B26A-4E29-BFB4-XXXXXXXXXXXX", i, i)
			ingest = fmt.Sprintf("%d%d-01-01 00:00:01.000 UTC", i, i)
			occurred = fmt.Sprintf("2000-%d%d-01 00:00:02.000 UTC", i, i)
			rawData = fmt.Sprintf("%d%d%s", i, i, filler)
		} else {
			// write a normal line
			uuid = fmt.Sprintf("%d-BA882B47-B26A-4E29-BFB4-XXXXXXXXXXXX", i)
			ingest = fmt.Sprintf("%d-01-01 00:00:01.000 UTC", i)
			occurred = fmt.Sprintf("2000-%d-01 00:00:02.000 UTC", i)
			rawData = fmt.Sprintf("%d%s", i, filler)
		}
		w.Write([]string{uuid, ingest, occurred, rawData})
	}

	w.Flush()
	return nil
}
