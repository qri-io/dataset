package dataset

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

// DatasetFilepaths looks for dataset.json files
func DatasetFilepaths(path string) (paths []string, err error) {
	return filepath.Glob(filepath.Join(path, "*/"+DatasetFilename))
}

// ReadDatasetPaths takes a slice of paths to dataset.json files and parses them
func ReadDatasetPaths(paths []string, e error) (datasets []*Dataset, err error) {
	if e != nil {
		return nil, e
	}

	datasets = make([]*Dataset, len(paths))
	for i, path := range paths {
		data, err := ioutil.ReadFile(path)
		d := &Dataset{}
		if err = json.Unmarshal(data, d); err != nil {
			return nil, err
		}
		datasets[i] = d
	}
	return
}

// Subsets encompasses the methods for defining a dataset.
type Subsets struct {
	SubsetsFolder string
	Datasets      []*Dataset
}

func (d Subsets) List(path string) ([]*Dataset, error) {
	if d.Datasets != nil {
		return d.Datasets, nil
	}

	return ReadDatasetPaths(DatasetFilepaths(path))
}

func (d Subsets) Walk(depth int, fn WalkDatasetsFunc) error {
	for _, ds := range d.Datasets {
		if err := ds.WalkDatasets(depth, fn); err != nil {
			return err
		}
	}

	return nil
}

func (d *Subsets) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*d = Subsets{SubsetsFolder: s}
		return nil
	}

	ds := make([]*Dataset, 0)
	if err := json.Unmarshal(data, &ds); err != nil {
		return err
	}

	*d = Subsets{
		Datasets: ds,
	}
	return nil
}

func (d Subsets) MarshalJSON() ([]byte, error) {
	if d.SubsetsFolder != "" {
		return []byte(fmt.Sprintf(`%s`, d.SubsetsFolder)), nil
	}
	return json.Marshal(d.Datasets)
}
