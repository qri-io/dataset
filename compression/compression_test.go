package compression

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	if _, err := Compressor("invalid", &bytes.Buffer{}); err == nil {
		t.Error("expected error constructing with invalid compression format string")
	}

	if _, err := Decompressor("invalid", &bytes.Buffer{}); err == nil {
		t.Error("expected error constructing with invalid decompression format string")
	}

	SupportedFormats[Format("invalid")] = struct{}{}
	defer delete(SupportedFormats, Format("invalid"))

	if _, err := Compressor("invalid", &bytes.Buffer{}); err == nil {
		t.Error("expected error constructing with compression format without backing compressor")
	}

	if _, err := Decompressor("invalid", &bytes.Buffer{}); err == nil {
		t.Error("expected error constructing with decompression format without backing decompressor")
	}
}

func TestCompressionCycle(t *testing.T) {
	for f := range SupportedFormats {
		t.Run(string(f), func(t *testing.T) {
			plainText := "I am a string destined to go through a compression spin cycle"

			buf := &bytes.Buffer{}
			comp, err := Compressor(f.String(), buf)
			if err != nil {
				t.Fatal(err)
			}

			if copied, err := io.Copy(comp, strings.NewReader(plainText)); err != nil {
				t.Fatal(err)
			} else if copied != int64(len([]byte(plainText))) {
				t.Errorf("copy byte length mismatch. want: %d got: %d", len(plainText), copied)
			}

			if err := comp.Close(); err != nil {
				t.Fatal(err)
			}

			if buf.String() == plainText {
				t.Errorf("buf contents should be compressed, unequal to plain text")
			}

			t.Log(buf.String())

			decomp, err := Decompressor(f.String(), buf)
			if err != nil {
				t.Fatal(err)
			}
			defer decomp.Close()

			result := &bytes.Buffer{}
			if _, err := io.Copy(result, decomp); err != nil {
				t.Fatal(err)
			}

			if result.String() != plainText {
				t.Errorf("compression roun trip result mismatch.\nwant: %s\ngot: %s", plainText, result.String())
			}
		})

	}

}
