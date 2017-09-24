package dsfs

import (
	"github.com/ipfs/go-ipfs/commands/files"
	"io/ioutil"
)

func fileBytes(file files.File, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(file)
}
