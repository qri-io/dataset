package dataset

import (
	"encoding/json"
	"fmt"

	"testing"
)

func QueryEqual(a, b *Query) error {
	if a.Format != b.Format {
		return fmt.Errorf("format mismatch: %s != %s", a.Format, b.Format)
	}
	if a.Statement != b.Statement {
		return fmt.Errorf("statement mismatch: %s != %s", a.Statement, b.Statement)
	}

	return nil
}

func TestQueryUnmarshalJSON(t *testing.T) {
	cases := []struct {
		str   string
		query *Query
		err   error
	}{
		{`"select a from b"`, &Query{Statement: "select a from b"}, nil},
		{`{ "statement" : "select a from b" }`, &Query{Statement: "select a from b"}, nil},
		{`{ "format" : "ql", "statement" : "select a from b" }`, &Query{Format: "ql", Statement: "select a from b"}, nil},
	}

	for i, c := range cases {
		got := &Query{}
		if err := json.Unmarshal([]byte(c.str), got); err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if err := QueryEqual(c.query, got); err != nil {
			t.Errorf("case %d query mismatch: %s", i, err)
			continue
		}
	}
}

func TestQueryMarshalJSON(t *testing.T) {
	cases := []struct {
		q   *Query
		out string
		err error
	}{
		{&Query{Statement: "select a from b"}, `"select a from b"`, nil},
		{&Query{Format: "ql", Statement: "select a from b"}, `{"format":"ql","statement":"select a from b"}`, nil},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.q)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if string(data) != c.out {
			t.Errorf("case %d result mismatch. expected: %s, got: %s", i, c.out, string(data))
			continue
		}
	}
}
