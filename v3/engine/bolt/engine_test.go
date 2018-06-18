package bolt

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/asdine/storm/v3/engine"
	"github.com/stretchr/testify/require"
)

func tempDB(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "stormv3")
	require.NoError(t, err)
	return path.Join(dir, "test.db"), func() {
		os.RemoveAll(dir)
	}
}

func TestEngine(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	e, err := NewEngine(path)
	require.NoError(t, err)

	tx, err := e.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	var buff engine.FieldBuffer

	err = buff.SetString("Name", "Hello")
	require.NoError(t, err)

	err = buff.SetInt64("Age", 10)
	require.NoError(t, err)

	_, err = tx.Insert(&buff, "a", "b")
	require.NoError(t, err)
}
