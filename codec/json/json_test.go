package json

import (
	"testing"

	"github.com/crashPopeye/storm/codec/internal"
)

func TestJSON(t *testing.T) {
	internal.RoundtripTester(t, Codec)
}
