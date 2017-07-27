package dataset

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/multiformats/go-multihash"
)

// CalcHash calculates the hash of a json.Marshaler
// TODO - this will have to place nice with IPFS block hashing strategies
func CalcHash(m json.Marshaler) (hash string, err error) {
	h := sha256.New()

	// marshal to cannoncical JSON representation
	data, err := m.MarshalJSON()
	if err != nil {
		return
	}
	if _, err = h.Write(data); err != nil {
		return
	}

	// mhBuf, err := multihash.EncodeName(h.Sum(nil), "sha2-256")
	mhBuf, err := multihash.Encode(h.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return
	}

	hash = hex.EncodeToString(mhBuf)
	return
}
