package dataset

import (
	"encoding/json"
	"fmt"

	"testing"
)

func QueryEqual(a, b *Query) error {
	if a.Syntax != b.Syntax {
		return fmt.Errorf("syntax mismatch: %s != %s", a.Syntax, b.Syntax)
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
		{`{ "syntax" : "ql", "statement" : "select a from b" }`, &Query{Syntax: "ql", Statement: "select a from b"}, nil},
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
		{&Query{Syntax: "sql", Statement: "select a from b"}, `{"resources":null,"schema":null,"statement":"select a from b","syntax":"sql"}`, nil},
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
