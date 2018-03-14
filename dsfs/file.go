package dsfs

import (
	"encoding/json"
	"io/ioutil"

	"github.com/qri-io/cafs"
)

// JSONFile is a convenenience method for creating a file from a json.Marshaller
func JSONFile(name string, m json.Marshaler) (cafs.File, error) {
	data, err := m.MarshalJSON()
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	return cafs.NewMemfileBytes(name, data), nil
}

func fileBytes(file cafs.File, err error) ([]byte, error) {
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	return ioutil.ReadAll(file)
}
