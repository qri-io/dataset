package subset

import (
	"testing"
	"time"

	"github.com/qri-io/dataset"

	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset/dsfs"
	"github.com/qri-io/dataset/dstest"
)

func addMovies(t *testing.T, s cafs.Filestore) string {
	prev := dsfs.Timestamp
	dsfs.Timestamp = func() time.Time { return time.Time{}.UTC() }
	defer func() {
		dsfs.Timestamp = prev
	}()

	tc, err := dstest.NewTestCaseFromDir("testdata/movies")
	if err != nil {
		t.Fatal(err)
	}

	path, err := dsfs.CreateDataset(s, tc.Input, nil, tc.BodyFile(), nil, dstest.PrivKey, true)
	if err != nil {
		t.Fatal(err)
	}

	return path
}

func TestLoadPreview(t *testing.T) {
	s := cafs.NewMapstore()
	path := addMovies(t, s)

	res, err := LoadPreview(s, path)
	if err != nil {
		t.Error(err)
	}

	expect := "3f4b42ad33241e81da3e874e772cbf99d0e7c949"
	sum := dstest.DatasetPodChecksum(res)
	if expect != sum {
		t.Errorf("dataset pod checksum mismatch. expected: %s, got: %s", expect, sum)
	}
}

func TestPreview(t *testing.T) {
	p := Preview(&dataset.DatasetPod{})

	expect := "a909a887caab333296f92c25e308e66c14d33480"
	sum := dstest.DatasetPodChecksum(p)
	if expect != sum {
		t.Errorf("empty preview checksum mismatch. expected: %s, got: %s", expect, sum)
	}

	p = Preview(&dataset.DatasetPod{Name: "a", Peername: "b", Path: "c"})

	expect = "ac6225bf511631200bdbb2200554472909d56ca8"
	sum = dstest.DatasetPodChecksum(p)
	if expect != sum {
		t.Errorf("preview with ref details mismatch. expected: %s, got: %s", expect, sum)
	}
}
