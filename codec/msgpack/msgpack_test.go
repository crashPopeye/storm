package msgpack

import (
	"testing"

	"github.com/crashPopeye/storm/codec/internal"
)

func TestMsgpack(t *testing.T) {
	internal.RoundtripTester(t, Codec)
}
