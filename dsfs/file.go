package dsfs

import (
	"encoding/json"
	"github.com/ipfs/go-ipfs/commands/files"
	"github.com/qri-io/cafs/memfs"
	"io/ioutil"
)

func fileBytes(file files.File, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(file)
}

func jsonFile(name string, m json.Marshaler) (files.File, error) {
	data, err := m.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return memfs.NewMemfileBytes(name, data), nil
}
