package bolt

import (
	"fmt"
	"testing"

	"github.com/asdine/storm/v3/engine"
	"github.com/stretchr/testify/require"
)

func TestBucket(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	e, err := NewEngine(path)
	require.NoError(t, err)
	tx, err := e.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	var buff engine.FieldBuffer
	for i := 0; i < 10; i++ {
		buff.Reset()

		err = buff.SetString("Name", fmt.Sprintf("Name %d", i))
		require.NoError(t, err)

		err = buff.SetInt64("Age", int64(i))
		require.NoError(t, err)

		_, err = tx.Insert(&buff, "a")
		require.NoError(t, err)
	}

	b, err := tx.Bucket("a")
	require.NoError(t, err)
	c, err := b.Cursor()
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		r, err := c.Next()
		require.NoError(t, err)
		require.NotNil(t, r)

		rs := engine.RecordScanner{Record: r}

		name, err := rs.GetString("Name")
		require.NoError(t, err)

		require.Equal(t, fmt.Sprintf("Name %d", i), name)

		age, err := rs.GetInt64("Age")
		require.NoError(t, err)
		require.Equal(t, int64(i), age)
	}

	r, err := c.Next()
	require.NoError(t, err)
	require.Nil(t, r)
}
