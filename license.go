package datapackage

import (
	"encoding/json"
	"fmt"
)

type License struct {
	Type string `json:"type"`
	Url  string `json:"url,omitempty"`
}

type _license License

func (l License) MarshalJSON() ([]byte, error) {
	if l.Type != "" && l.Url == "" {
		return []byte(fmt.Sprintf(`"%s"`, l.Type)), nil
	}

	return json.Marshal(_license(l))
}

func (l *License) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*l = License{Type: s}
		return nil
	}

	_l := &_license{}
	if err := json.Unmarshal(data, _l); err != nil {
		return err
	}
	*l = License(*_l)

	return nil
}
