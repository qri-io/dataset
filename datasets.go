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

type Datasets struct {
	folder   string
	datasets []*Dataset
}

func (d Datasets) List(path string) ([]*Dataset, error) {
	if d.datasets != nil {
		return d.datasets, nil
	}

	return ReadDatasetPaths(DatasetFilepaths(path))
}

func (d Datasets) Walk(depth int, fn WalkDatasetsFunc) error {

	for _, ds := range d.datasets {
		if err := ds.WalkDatasets(depth, fn); err != nil {
			return err
		}
	}

	return nil
}

func (d *Datasets) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*d = Datasets{folder: s}
		return nil
	}

	// _d := &_datasets
	ds := make([]*Dataset, 0)
	if err := json.Unmarshal(data, &ds); err != nil {
		return err
	}

	*d = Datasets{
		datasets: ds,
	}
	return nil
}

func (d Datasets) MarshalJSON() ([]byte, error) {
	if d.folder != "" {
		return []byte(fmt.Sprintf(`%s`, d.folder)), nil
	}
	return json.Marshal(d.datasets)
}
