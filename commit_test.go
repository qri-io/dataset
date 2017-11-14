package dataset

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ipfs/go-datastore"
)

func TestCommitMsgMarshalJSON(t *testing.T) {
	cases := []struct {
		in  *CommitMsg
		out []byte
		err error
	}{
		{&CommitMsg{Title: "title"}, []byte(`{"title":"title"}`), nil},
		{&CommitMsg{Author: &User{Id: "foo"}}, []byte(`{"author":{"id":"foo"},"title":""}`), nil},
		// {AirportCodes, []byte(`{"format":"csv","formatConfig":{"header_row":true},"path":"","query":"","schema":{"fields":[{"name":"ident","type":"string"},{"name":"type","type":"string"},{"name":"name","type":"string"},{"name":"latitude_deg","type":"float"},{"name":"longitude_deg","type":"float"},{"name":"elevation_ft","type":"integer"},{"name":"continent","type":"string"},{"name":"iso_country","type":"string"},{"name":"iso_region","type":"string"},{"name":"municipality","type":"string"},{"name":"gps_code","type":"string"},{"name":"iata_code","type":"string"},{"name":"local_code","type":"string"}]}}`), nil},
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

	strbytes, err := json.Marshal(&CommitMsg{path: datastore.NewKey("/path/to/dataset")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/dataset\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/dataset\"")
	}
}
