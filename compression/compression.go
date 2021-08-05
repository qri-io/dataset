// Package compression presents a uniform interface for a set of compression
// readers & writers in various formats
package compression

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

const (
	// FmtNone is a sentinel for no compression
	FmtNone Format = ""
	// FmtZStandard compression https://facebook.github.io/zstd/
	FmtZStandard Format = "zst"
	// FmtGZip GNU zip compression https://www.gnu.org/software/gzip/
	FmtGZip Format = "gzip"
)

// Format represents a type of byte compression
type Format string

// String implements the stringer interface
func (s Format) String() string {
	return string(s)
}

// SupportedFormats indexes supported formats in a map for lookups
var SupportedFormats = map[Format]struct{}{
	FmtZStandard: {},
	FmtGZip:      {},
}

// ParseFormat interprets a string into a supported compression format
// errors when provided the empty string ("no compression" format)
func ParseFormat(s string) (f Format, err error) {
	f, ok := map[string]Format{
		"gzip": FmtGZip,
		"gz":   FmtGZip,
		"zst":  FmtZStandard,
		"zstd": FmtZStandard, // not a common file ending, but "zstd" is the shorthand name for the library
	}[s]

	if !ok {
		return f, fmt.Errorf("invalid compression format %q", s)
	}

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
		return zstdReadCloserShim{rdr}, nil
	case FmtGZip:
		return gzip.NewReader(r)
	}

	return nil, fmt.Errorf("no available decompressor for %q format", f)
}

// small struct to compensate for zstd's decoder Close() method, which returns
// no error. This breaks the io.ReadCloser interface. shim in an
// error function with an error that will never occur
type zstdReadCloserShim struct {
	*zstd.Decoder
}

func (d zstdReadCloserShim) Close() error {
	d.Decoder.Close()
	return nil
}
