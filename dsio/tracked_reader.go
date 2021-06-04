package dsio

import "io"

// TrackedReader wraps a reader, keeping an internal count of the bytes read
type TrackedReader struct {
	read int
	r    io.Reader
}

// NewTrackedReader creates a new tracked reader
func NewTrackedReader(r io.Reader) *TrackedReader {
	return &TrackedReader{r: r}
}

// Read implements the io.Reader interface
func (tr *TrackedReader) Read(p []byte) (n int, err error) {
	n, err = tr.r.Read(p)
	tr.read += n
	return
}

// BytesRead gives the total number of bytes read from the underlying reader
func (tr *TrackedReader) BytesRead() int {
	return tr.read
}

// Close implements the io.Closer interface, closes the underlying reader if
// it's an io.Closer
func (tr *TrackedReader) Close() error {
	if cl, ok := tr.r.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}
