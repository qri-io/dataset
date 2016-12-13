package dataset

import (
	"encoding/json"
	"fmt"
)

type Query struct {
	Format    string `json:"format,omitempty"`
	Statement string `json:"statement,omitempty"`
}

type _query Query

func (q *Query) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		// return fmt.Errorf("Data Type should be a string, got %s", data)
		*q = Query{Statement: s}
		return nil
	}

	_q := &_query{}
	if err := json.Unmarshal(data, _q); err != nil {
		return err
	}

	*q = Query(*_q)
	return nil
}

func (q Query) MarshalJSON() ([]byte, error) {
	if q.Statement != "" && q.Format == "" {
		return []byte(fmt.Sprintf(`"%s"`, q.Statement)), nil
	}
	return json.Marshal(_query(q))
}
