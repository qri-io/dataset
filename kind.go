package dataset

import (
	"encoding/json"
	"fmt"
)

// CurrentSpecVersion is the current verion of the dataset spec
const CurrentSpecVersion = "0"

const (
	// KindDataset is the current kind for datasets
	KindDataset = Kind("ds:" + CurrentSpecVersion)
	// KindMeta is the current kind for metadata
	KindMeta = Kind("md:" + CurrentSpecVersion)
	// KindStructure is the current kind for dataset structures
	KindStructure = Kind("st:" + CurrentSpecVersion)
	// KindTransform is the current kind for dataset transforms
	KindTransform = Kind("tf:" + CurrentSpecVersion)
	// KindCommit is the current kind for dataset transforms
	KindCommit = Kind("cm:" + CurrentSpecVersion)
	// KindVisConfig is the current kind for dataset transforms
	KindVisConfig = Kind("vs" + CurrentSpecVersion)
)

// Kind is a short identifier for all types of qri dataset objects
// Kind does three things:
// 1. Distinguish qri datasets from other formats
// 2. Distinguish different types (Dataset/Structure/Transform/etc.)
// 3. Distinguish between versions of the dataset spec
// Kind is a string in the format ds:[version]
type Kind string

// String implements the stringer interface
func (k Kind) String() string {
	return string(k)
}

// Valid checks to see if a kind string is valid
func (k Kind) Valid() error {
	if len(k) < 4 {
		return fmt.Errorf("invalid kind: '%s'. kind must be in the form [type]:[version]", k.String())
	}
	return nil
}

// Type returns the type identifier
func (k Kind) Type() string {
	return k.String()[:2]
}

// Version returns the version portion of the kind identifier
func (k Kind) Version() string {
	return k.String()[3:]
}

// UnmarshalJSON implements the JSON.Unmarshaler interface,
// rejecting any strings that are not a valid kind
func (k *Kind) UnmarshalJSON(data []byte) error {
	var _k string
	if err := json.Unmarshal(data, &_k); err != nil {
		return err
	}
	*k = Kind(_k)
	return k.Valid()
}
