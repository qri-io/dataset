package dsfs

import (
	"encoding/json"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/cafs/memfile"
	"github.com/qri-io/dataset"
)

// func (q *Query) LoadStructures(store datastore.Datastore) (structs map[string]*Structure, err error) {
//  structs = map[string]*Structure{}
//  for key, path := range q.Structures {
//    s, err := LoadStructure(store, path)
//    if err != nil {
//      return nil, err
//    }
//    structs[key] = s
//  }
//  return
// }

// func (q *Query) LoadAbstractStructures(store datastore.Datastore) (structs map[string]*Structure, err error) {
//  structs = map[string]*Structure{}
//  for key, path := range q.Structures {
//    s, err := LoadStructure(store, path)
//    if err != nil {
//      return nil, err
//    }
//    structs[key] = s.Abstract()
//  }
//  return
// }

// LoadQuery loads a query from a given path in a store
func LoadQuery(store cafs.Filestore, path datastore.Key) (q *dataset.Query, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		return nil, err
	}

	return dataset.UnmarshalQuery(data)
}

func SaveQuery(store cafs.Filestore, q *dataset.Query, pin bool) (datastore.Key, error) {
	if q == nil {
		return datastore.NewKey(""), nil
	}

	// *don't* need to break query out into different structs.
	// stpath, err := q.Structure.Save(store)
	// if err != nil {
	//  return datastore.NewKey(""), err
	// }

	qdata, err := json.Marshal(q)
	if err != nil {
		return datastore.NewKey(""), err
	}

	return store.Put(memfile.NewMemfileBytes("query.json", qdata), pin)
}
