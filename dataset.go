package dataset

import (
	"encoding/json"
	"fmt"
)

// Current version of the specification
const version = "0.0.1"

// Dataset combines Metadata & Resource to form a "full" description
type Dataset struct {
	Metadata
	Resource
}

// UnmarshalResource tries to extract a resource type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalDataset(v interface{}) (*Dataset, error) {
	switch r := v.(type) {
	case *Dataset:
		return r, nil
	case Dataset:
		return &r, nil
	case []byte:
		dataset := &Dataset{}
		err := json.Unmarshal(r, dataset)
		return dataset, err
	default:
		return nil, fmt.Errorf("couldn't parse dataset")
	}
}
