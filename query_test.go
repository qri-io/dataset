package dataset

import (
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-datastore"

	"testing"
)

func CompareQuery(a, b *Query) error {
	if a.Syntax != b.Syntax {
		return fmt.Errorf("syntax mismatch: %s != %s", a.Syntax, b.Syntax)
	}
	if a.Statement != b.Statement {
		return fmt.Errorf("statement mismatch: %s != %s", a.Statement, b.Statement)
	}

	return nil
}

func TestLoadQuery(t *testing.T) {
	store := datastore.NewMapDatastore()
	a := datastore.NewKey("/straight/value")
	if err := store.Put(a, &Query{Statement: "select * from whatever booooooo go home"}); err != nil {
		t.Errorf(err.Error())
		return
	}

	_, err := LoadQuery(store, a)
	if err != nil {
		t.Errorf(err.Error())
	}
	// TODO - other tests & stuff
}

func TestQueryLoadAbstractStructures(t *testing.T) {
	// store := datastore.NewMapDatastore()
	// TODO - finish dis test
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

		if err := CompareQuery(c.query, got); err != nil {
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
		{&Query{Syntax: "sql", Statement: "select a from b"}, `{"outputStructure":"","statement":"select a from b","structures":null,"syntax":"sql"}`, nil},
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
