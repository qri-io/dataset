package dsfs

import (
	"testing"

	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

var Viz1 = &dataset.Viz{
	Format:     "foo",
	Qri:        dataset.KindViz,
	ScriptPath: "bar",
}

func TestLoadViz(t *testing.T) {
	store := cafs.NewMapstore()
	a, err := SaveViz(store, Viz1, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if _, err := LoadViz(store, a); err != nil {
		t.Errorf(err.Error())
	}
}
