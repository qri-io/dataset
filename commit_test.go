package dataset

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestCommit(t *testing.T) {
	ref := NewCommitRef("a")
	if !ref.IsEmpty() {
		t.Errorf("expected reference to be empty")
	}

	if ref.Path != "a" {
		t.Errorf("expected ref path to equal /a")
	}
}

func TestCommitAssign(t *testing.T) {
	t1 := time.Now()
	doug := &User{ID: "doug_id", Email: "doug@example.com"}
	expect := &Commit{
		Path:      "a",
		Qri:       KindCommit.String(),
		Author:    doug,
		Timestamp: t1,
		Title:     "expect title",
		Message:   "expect message",
		Signature: "sig",
	}
	got := &Commit{
		Author:  &User{ID: "maha_id", Email: "maha@example.com"},
		Title:   "title",
		Message: "message",
	}

	got.Assign(&Commit{
		Author: doug,
		Qri:    KindCommit.String(),
		Title:  "expect title",
	}, &Commit{
		Path:      "a",
		Timestamp: t1,
		Message:   "expect message",
		Signature: "sig",
	})

	if err := CompareCommits(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareCommits(expect, got); err != nil {
		t.Error(err)
	}

	emptyMsg := &Commit{}
	emptyMsg.Assign(expect)
	if err := CompareCommits(expect, emptyMsg); err != nil {
		t.Error(err)
	}
}

func TestCommitIsEmpty(t *testing.T) {
	cases := []struct {
		cm *Commit
	}{
		{&Commit{Title: "a"}},
		{&Commit{Author: &User{}}},
		{&Commit{Message: "a"}},
		{&Commit{Signature: "a"}},
		{&Commit{Timestamp: time.Now()}},
	}

	for i, c := range cases {
		if c.cm.IsEmpty() == true {
			t.Errorf("case %d improperly reported commit as empty", i)
			continue
		}
	}
}

func TestCommitMarshalJSON(t *testing.T) {
	ts := time.Date(2001, 01, 01, 01, 01, 01, 0, time.UTC)
	cases := []struct {
		in  *Commit
		out []byte
		err error
	}{
		{&Commit{Title: "title", Timestamp: ts}, []byte(`{"qri":"cm:0","timestamp":"2001-01-01T01:01:01Z","title":"title"}`), nil},
		{&Commit{Author: &User{ID: "foo"}, Timestamp: ts}, []byte(`{"author":{"id":"foo"},"qri":"cm:0","timestamp":"2001-01-01T01:01:01Z","title":""}`), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if !bytes.Equal(c.out, got) {
			t.Errorf("case %d error mismatch. %s != %s", i, string(c.out), string(got))
			continue
		}
	}

	strbytes, err := json.Marshal(&Commit{Path: "/path/to/dataset"})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/dataset\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/dataset\"")
	}
}

func TestCommitMarshalJSONObject(t *testing.T) {
	ts := time.Date(2001, 01, 01, 01, 01, 01, 0, time.UTC)
	cases := []struct {
		in  *Commit
		out []byte
		err error
	}{
		{&Commit{Title: "title", Timestamp: ts}, []byte(`{"qri":"cm:0","timestamp":"2001-01-01T01:01:01Z","title":"title"}`), nil},
		{&Commit{Author: &User{ID: "foo"}, Timestamp: ts}, []byte(`{"author":{"id":"foo"},"qri":"cm:0","timestamp":"2001-01-01T01:01:01Z","title":""}`), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		check := &map[string]interface{}{}
		err = json.Unmarshal(got, check)
		if err != nil {
			t.Errorf("case %d error: failed to unmarshal to object: %s", i, err.Error())
			continue
		}

	}

}

func TestCommitUnmarshalJSON(t *testing.T) {
	cases := []struct {
		data   string
		result *Commit
		err    string
	}{
		{`{}`, &Commit{}, ""},
		{`{ "title": "title", "message": "message"}`, &Commit{Title: "title", Message: "message"}, ""},
		{`{ "author" : { "id": "id", "email": "email@email.com"} }`, &Commit{Author: &User{ID: "id", Email: "email@email.com"}}, ""},
		{`{`, &Commit{Author: &User{ID: "id", Email: "email@email.com"}}, "error unmarshling commit: unexpected end of JSON input"},
	}

	for i, c := range cases {
		cm := &Commit{}
		err := cm.UnmarshalJSON([]byte(c.data))
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err := CompareCommits(cm, c.result); err != nil {
			t.Errorf("case %d comparison error: %s", i, err)
			continue
		}
	}

	strq := &Commit{}
	path := "/path/to/msg"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strq); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strq.Path != path {
		t.Errorf("unmarshal didn't set proper Path: %s != %s", path, strq.Path)
		return
	}
}

func TestUnmarshalCommit(t *testing.T) {
	cma := Commit{Qri: KindCommit.String(), Message: "foo"}
	cases := []struct {
		value interface{}
		out   *Commit
		err   string
	}{
		{cma, &cma, ""},
		{&cma, &cma, ""},
		{[]byte("{\"qri\":\"cm:0\"}"), &Commit{Qri: KindCommit.String()}, ""},
		{5, nil, "couldn't parse commitMsg, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalCommit(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareCommits(c.out, got); err != nil {
			t.Errorf("case %d dataset mismatch: %s", i, err.Error())
			continue
		}
	}
}
