package dsfs

import (
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	ipfsfs "github.com/qri-io/cafs/ipfs"
	"github.com/qri-io/cafs/memfs"
)

func TestPackageFilepath(t *testing.T) {
	ipfs, err := ipfsfs.NewFilestore()
	if err != nil {
		t.Errorf("error creating ipfs filestore: %s", err.Error())
		return
	}

	mem := memfs.NewMapstore()

	cases := []struct {
		store cafs.Filestore
		path  string
		pf    PackageFile
		out   string
	}{
		{ipfs, "/ipfs/foo", PackageFileDataset, "/ipfs/foo/dataset.json"},
		{ipfs, "/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M", PackageFileDataset, "/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json"},
		{ipfs, "/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json", PackageFileDataset, "/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json"},
		{ipfs, "/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json", PackageFileMeta, "/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/meta.json"},
		{ipfs, "QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M", PackageFileDataset, "/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json"},

		{mem, "/mem/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M", PackageFileDataset, "/mem/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M"},
		{mem, "/mem/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json", PackageFileDataset, "/mem/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json"},
		{mem, "/mem/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json", PackageFileMeta, "/mem/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/dataset.json"},
	}

	for i, c := range cases {
		got := PackageFilepath(c.store, c.path, c.pf)
		if got != c.out {
			t.Errorf("case %d result mismatch. expected: '%s', got: '%s'", i, c.path, c.pf)
			continue
		}
	}
}

func TestPackageKeyPath(t *testing.T) {
	mem := memfs.NewMapstore()
	p := datastore.NewKey("/mem/foo")
	got := PackageKeypath(mem, p, PackageFileDataset)
	if !got.Equal(p) {
		t.Errorf("key mismatch. expected: %s, got %s", p, got)
	}
}
