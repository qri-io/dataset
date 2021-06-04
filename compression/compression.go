// Package compression is a horrible hack & should be replaced
// as soon as humanly possible
package compression

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

// Format represents a type of byte compression
type Format string

// String implements the stringer interface
func (s Format) String() string {
	return string(s)
}

const (
	// FmtNone is a sentinel for no compression
	FmtNone Format = ""
	// FmtZStandard compression https://facebook.github.io/zstd/
	FmtZStandard Format = "zstd"
	// FmtGZip GNU zip compression https://www.gnu.org/software/gzip/
	FmtGZip Format = "gzip"
)

// SupportedFormats indexes suppoorted formats in a map for lookups
var SupportedFormats = map[Format]struct{}{
	FmtZStandard: {},
	FmtGZip:      {},
}

// ParseFormat interprets a string into a supported compression format
// errors when provided the empty string ("no compression" format)
func ParseFormat(s string) (f Format, err error) {
	f = Format(s)
	if _, ok := SupportedFormats[f]; !ok {
		return FmtNone, fmt.Errorf("unsupported compression format: %q", s)
	}
	return f, nil
}

// Compressor wraps a given writer with a specified comrpession format
// callers must Close the writer to fully flush the compressor
func Compressor(compressionFormat string, w io.Writer) (io.WriteCloser, error) {
	f, err := ParseFormat(compressionFormat)
	if err != nil {
		return nil, err
	}

	switch f {
	case FmtZStandard:
		return zstd.NewWriter(w)
	case FmtGZip:
		return gzip.NewWriter(w), nil
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
	case FmtZStandard:
		rdr, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return zstdErrorDecoderShim{rdr}, nil
	case FmtGZip:
		return gzip.NewReader(r)
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
