package dsfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/qri-io/cafs"
	ipfsfs "github.com/qri-io/cafs/ipfs"
)

func TestPackageFilepath(t *testing.T) {
	ipfs, destroy, err := makeTestIPFSRepo("")
	if err != nil {
		t.Errorf("error creating IPFS test repo: %s", err.Error())
		return
	}
	defer destroy()

	mem := cafs.NewMapstore()

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

func makeTestIPFSRepo(path string) (fs *ipfsfs.Filestore, destroy func(), err error) {
	if path == "" {
		path = filepath.Join(os.TempDir(), ".ipfs")
	}
	err = ipfsfs.InitRepo(path, "")
	if err != nil {
		return
	}
	fs, err = ipfsfs.NewFilestore(func(cfg *ipfsfs.StoreCfg) { cfg.FsRepoPath = path })
	if err != nil {
		return
	}

	destroy = func() {
		os.RemoveAll(path)
	}

	return
}
