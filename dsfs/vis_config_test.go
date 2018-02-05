package dsfs

import (
	"testing"

	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

var VisConfig1 = &dataset.VisConfig{
	Format: "foo",
	Kind:   dataset.KindVisConfig,
	Visualizations: map[string]interface{}{
		"type": "bar",
		"colors": map[string]interface{}{
			"bars":       "#ffffff",
			"background": "#000000",
		},
	},
}

func TestLoadVisConfig(t *testing.T) {
	store := memfs.NewMapstore()
	a, err := SaveVisConfig(store, VisConfig1, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadVisConfig(store, a); err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}
