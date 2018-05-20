package dsio

import (
	"strings"
	"testing"
)

func TestTrackedReader(t *testing.T) {
	r := strings.NewReader("0123456789")
	tr := NewTrackedReader(r)

	buf := make([]byte, 4)
	tr.Read(buf)
	if tr.BytesRead() != 4 {
		t.Errorf("expected bytes read to equal 4, got: %d", tr.BytesRead())
	}
	tr.Read(buf)
	if tr.BytesRead() != 8 {
		t.Errorf("expected bytes read to equal 4, got: %d", tr.BytesRead())
	}
	tr.Read(buf)
	if tr.BytesRead() != 10 {
		t.Errorf("expected bytes read to equal 4, got: %d", tr.BytesRead())
	}
}
