package dsgraph

import (
	"encoding/json"
	"github.com/ipfs/go-datastore"
)

// TransformResults graphs transform paths to result paths
type TransformResults map[datastore.Key][]datastore.Key

// AddResult adds a result to the TransformResults map
func (qr TransformResults) AddResult(transform, result datastore.Key) {
	for _, r := range qr[transform] {
		if r.Equal(result) {
			return
		}
	}
	qr[transform] = append(qr[transform], result)
}

// MarshalJSON implements the json.Marshaler interface for TransformResults
func (qr TransformResults) MarshalJSON() ([]byte, error) {
	qrmap := map[string]interface{}{}
	for key, vals := range qr {
		strs := make([]string, len(vals))
		for i, v := range vals {
			strs[i] = v.String()
		}
		qrmap[key.String()] = strs
	}
	return json.Marshal(qrmap)
}

// UnmarshalJSON implements the json.Unmarshaler interface for TransformResults
func (qr *TransformResults) UnmarshalJSON(data []byte) error {
	qrmap := map[string][]datastore.Key{}
	if err := json.Unmarshal(data, &qrmap); err != nil {
		return err
	}

	r := TransformResults{}

	for key, vals := range qrmap {
		r[datastore.NewKey(key)] = vals
	}
	*qr = r
	return nil
}
