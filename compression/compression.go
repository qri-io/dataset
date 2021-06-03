// Package compression is a horrible hack & should be replaced
// as soon as humanly possible
package compression

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Format represents a type of byte compression
type Format string

// String implements the stringer interface
func (s Format) String() string {
	return string(s)
}

const (
	// ZStandard compression https://facebook.github.io/zstd/
	ZStandard Format = "zstd"
)

// Supported formats indexes suppoorted formats in a map for easy lookups
var SupportedFormats = map[Format]struct{}{
	ZStandard: {},
}

// ParseFormat interprets a string into a compression format
func ParseFormat(t string) (f Format, err error) {
	f = Format(t)
	if _, ok := SupportedFormats[f]; !ok {
		err = fmt.Errorf("unsupported compression format: %q", t)
	}
	return f, err
}

// Compressor wraps a given writer with a specified comrpession format
// callers must Close the writer to fully flush the compressor
func Compressor(compressionFormat string, w io.Writer) (io.WriteCloser, error) {
	f, err := ParseFormat(compressionFormat)
	if err != nil {
		return nil, err
	}

	switch f {
	case ZStandard:
		return zstd.NewWriter(w)
	}

	return nil, fmt.Errorf("no available compressor for %q format", f)
}

// Decompressor wraps a reader of compressed data with a decompressor
// callers must .Close() the reader
func Decompressor(compressionFormat string, r io.Reader) (io.ReadCloser, error) {
	f, err := ParseFormat(compressionFormat)
	if err != nil {
		return nil, err
	}

	switch f {
	case ZStandard:
		rdr, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return zstdErrorDecoderShim{rdr}, nil
	}

	return nil, fmt.Errorf("no available decompressor for %q format", f)
}

// small struct to compensate for the the fact that zstd's decoder Close()
// method returns no error, breaking the io.ReadCloser interface. shim in an
// error function that will never occur
type zstdErrorDecoderShim struct {
	*zstd.Decoder
}

func (d zstdErrorDecoderShim) Close() error {
	d.Decoder.Close()
	return nil
}
