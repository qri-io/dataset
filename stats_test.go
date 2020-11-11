package dataset

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestStatsDropTransientValues(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestStatsDropDerivedValues(t *testing.T) {
	tf := &Stats{
		Path: "/ipfs/QmHash",
		Qri:  "oh you know it's qri",
	}

	tf.DropDerivedValues()

	if !cmp.Equal(tf, &Stats{}) {
		t.Errorf("expected dropping a struct only derived values to be empty")
	}
}

func TestStatsAssign(t *testing.T) {
	expect := &Stats{
		Path: "path",
		Qri:  "change",
		Stats: []map[string]interface{}{
			{"foo": "bar"},
		},
	}
	got := &Stats{}

	got.Assign(&Stats{
		Stats: []map[string]interface{}{
			{"foo": "bar"},
		},
	}, &Stats{
		Path: "path",
		Qri:  "change",
	})

	if err := CompareStats(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareStats(expect, got); err != nil {
		t.Error(err)
	}

	emptySa := &Stats{}
	emptySa.Assign(expect)
	if err := CompareStats(expect, emptySa); err != nil {
		t.Error(err)
	}
}

func TestStatsUnmarshalJSON(t *testing.T) {
	cases := []struct {
		str   string
		Stats *Stats
		err   string
	}{
		{`{}`, &Stats{}, ""},
		{`{"stats":{"foo": "/not/a/real/path"}}`, &Stats{Stats: map[string]interface{}{"foo": map[string]interface{}{"foo": "/not/a/real/path"}}}, ""},
	}

	for i, c := range cases {
		got := &Stats{}
		err := json.Unmarshal([]byte(c.str), got)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if err := CompareStats(c.Stats, got); err != nil {
			t.Errorf("case %d Stats mismatch: %s", i, err)
			continue
		}
	}

	strq := &Stats{}
	path := "/path/to/Stats"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strq); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strq.Path != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strq.Path)
		return
	}
}

func TestStatsMarshalJSONObject(t *testing.T) {
	cases := []struct {
		q   *Stats
		out string
	}{
		{&Stats{}, `{"qri":"sa:0"}`},
		{&Stats{Stats: "sql", Path: "path"}, `{"path":"path","qri":"sa:0","stats":"sql"}`},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.q)
		if err != nil {
			t.Errorf("case %d unexpected error: %q", i, err)
			continue
		}
		if string(data) != c.out {
			t.Errorf("case %d result mismatch. expected: %s, got: %s", i, c.out, string(data))
			continue
		}
	}

	strbytes, err := json.Marshal(&Stats{Path: "/path/to/Stats"})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte(`"/path/to/Stats"`)) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), `"/path/to/Stats"`)
	}
}

func TestStatsMarshalJSON(t *testing.T) {
	cases := []struct {
		q   *Stats
		out string
	}{
		{&Stats{}, `{"qri":"sa:0"}`},
		{&Stats{Stats: "sql", Path: "path"}, `{"path":"path","qri":"sa:0","stats":"sql"}`},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.q)
		if err != nil {
			t.Errorf("case %d unexpected error %q", i, err)
			continue
		}
		check := &map[string]interface{}{}
		err = json.Unmarshal(data, check)
		if err != nil {
			t.Errorf("case %d error: failed to unmarshal to object: %s", i, err.Error())
			continue
		}
	}

}

func TestStatsIsEmpty(t *testing.T) {
	cases := []struct {
		tf       *Stats
		expected bool
	}{
		{&Stats{Qri: KindStats.String()}, true},
		{&Stats{Path: "foo"}, true},
		{&Stats{}, true},
		{&Stats{Stats: "foo"}, false},
	}

	for i, c := range cases {
		if c.tf.IsEmpty() != c.expected {
			t.Errorf("case %d improperly reported Stats as empty == %v", i, c.expected)
			continue
		}
	}
}
