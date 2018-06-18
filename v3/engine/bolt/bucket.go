package bolt

import (
	"bytes"
	"fmt"

	"github.com/asdine/storm/v3/engine"
	bolt "github.com/coreos/bbolt"
)

type bucket struct {
	b      *bolt.Bucket
	schema *engine.Schema
}

func newBucket(bb *bolt.Bucket) (*bucket, error) {
	sb, err := newSchemaBucket(bb)
	if err != nil {
		return nil, err
	}

	schema, err := sb.Schema()
	if err != nil {
		return nil, err
	}

	b := bucket{
		b:      bb,
		schema: schema,
	}

	return &b, nil
}

func (b *bucket) Cursor() (engine.Cursor, error) {
	var c cursor
	c.cur = b.b.Cursor()
	c.k, c.v = c.cur.First()
	c.schema = b.schema

	return &c, nil
}

func (b *bucket) Schema() (*engine.Schema, error) {
	return b.schema, nil
}

type cursor struct {
	buff   engine.FieldBuffer
	cur    *bolt.Cursor
	k, v   []byte
	schema *engine.Schema
}

func (c *cursor) Next() (engine.Record, error) {
	if c.k == nil {
		return nil, nil
	}

	var id, fld, curid []byte

	c.buff.Reset()

	for c.k != nil {
		// skip buckets
		for c.k != nil && c.v == nil {
			c.k, c.v = c.cur.Next()
		}

		if c.k == nil {
			break
		}

		if idx := bytes.IndexByte(c.k, '-'); idx != -1 {
			id, fld = c.k[:idx], c.k[idx+1:]
		} else {
			return nil, fmt.Errorf("malformed rowid '%s'", c.k)
		}

		if curid != nil && !bytes.Equal(id, curid) {
			break
		}

		curid = id

		f := c.schema.Get(string(fld))
		err := c.buff.SetData(f.Name, f.Type, c.v)
		if err != nil {
			return nil, err
		}

		c.k, c.v = c.cur.Next()
	}

	if c.buff.Len() == 0 {
		return nil, nil
	}

	return &c.buff, nil
}
