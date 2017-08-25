package dataset

import (
	"github.com/ipfs/go-datastore"
)

// DatasetRef is a reference to a dataset. This is probably
// going to change in the near future.
type DatasetRef struct {
	Dataset *Dataset      `json:"dataset"`
	Name    string        `json:"name,omitempty"`
	Path    datastore.Key `json:"path"`
}
