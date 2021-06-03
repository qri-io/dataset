package compression

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

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
			defer comp.Close()

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
