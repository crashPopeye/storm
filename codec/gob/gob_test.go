package gob

import (
	"testing"

	"github.com/crashPopeye/storm/codec/internal"
)

func TestGob(t *testing.T) {
	internal.RoundtripTester(t, Codec)
}
