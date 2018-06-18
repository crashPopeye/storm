package bolt

import (
	"github.com/asdine/storm/v3/engine"
	bolt "github.com/coreos/bbolt"
)

type Engine struct {
	db *bolt.DB
}

// TODO options and stuff
func NewEngine(path string) (*Engine, error) {
	db, err := bolt.Open(path, 0660, nil)
	if err != nil {
		return nil, err
	}

	return &Engine{
		db: db,
	}, nil
}

func (e *Engine) Begin(writable bool) (engine.Transaction, error) {
	tx, err := e.db.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &transaction{tx}, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

type transaction struct {
	*bolt.Tx
}

func (t *transaction) Insert(r engine.Record, path ...string) (key []byte, err error) {
	b, err := createBucketIfNotExists(t.Tx, path)
	if err != nil {
		return nil, err
	}

	seq, err := b.NextSequence()
	if err != nil {
		return nil, err
	}

	rowid, err := engine.EncodeInt64(int64(seq))
	if err != nil {
		return nil, err
	}

	sb, err := newSchemaBucket(b)
	if err != nil {
		return nil, err
	}

	for {
		f, err := r.Next()
		if err != nil {
			return nil, err
		}

		if f == nil {
			break
		}

		err = sb.Set(f)
		if err != nil {
			return nil, err
		}

		v, err := f.Encode()
		if err != nil {
			return nil, err
		}

		k := append(rowid, '-')
		k = append(k, f.Name...)

		err = b.Put(k, v)
		if err != nil {
			return nil, err
		}
	}

	return rowid, err
}

func (t *transaction) Bucket(path ...string) (engine.Bucket, error) {
	b, err := createBucketIfNotExists(t.Tx, path)
	if err != nil {
		return nil, err
	}

	return newBucket(b)
}

func createBucketIfNotExists(tx *bolt.Tx, path []string) (b *bolt.Bucket, err error) {
	for _, p := range path {
		if b == nil {
			b, err = tx.CreateBucketIfNotExists([]byte(p))
		} else {
			b, err = b.CreateBucketIfNotExists([]byte(p))
		}

		if err != nil {
			return nil, err
		}
	}

	return b, err
}
