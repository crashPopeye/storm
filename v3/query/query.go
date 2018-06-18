package query

import (
	"errors"

	"github.com/asdine/storm/v3/engine"
)

type Query struct {
	pl engine.Pipeline
}

func (q *Query) Select() *Selector {
	var selector Selector

	q.pl = append(q.pl, &selector)

	return &selector
}

func (q *Query) Match(matchers ...Matcher) {
	var mp matcherPipe

	// TODO change that by wrapping all the matchers with an AND
	mp.matcher = matchers[0]
	q.pl = append(q.pl, &mp)
}

func (q *Query) Pipe(b engine.Bucket) (engine.Bucket, error) {
	return q.pl.Run(b)
}

type Selector struct {
	selectors []func(engine.Bucket, *engine.RecordBuffer, *engine.Schema) error
}

func (s *Selector) Pipe(b engine.Bucket) (engine.Bucket, error) {
	var buff engine.RecordBuffer

	schema, err := b.Schema()
	if err != nil {
		return nil, err
	}

	for _, slc := range s.selectors {
		err = slc(b, &buff, schema)
		if err != nil {
			return nil, err
		}
	}

	return &buff, nil
}

func (s *Selector) Field(name string) *Selector {
	return s.Select(func(b engine.Bucket, rb *engine.RecordBuffer, s *engine.Schema) error {
		c, err := b.Cursor()
		if err != nil {
			return err
		}

		for {
			r, err := c.Next()
			if err != nil {
				return err
			}

			if r == nil {
				break
			}

			f, err := selectField(r, name)
			if err != nil {
				return err
			}

			var fb engine.FieldBuffer
			err = fb.SetData(name, f.Type, f.Data)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func selectField(r engine.Record, name string) (*engine.Field, error) {
	for {
		f, err := r.Next()
		if err != nil {
			return nil, err
		}

		if f == nil {
			return nil, errors.New("select: field not found")
		}

		if f.Name == name {
			return f, nil
		}
	}
}

func (s *Selector) MaxInt64(field string) *Selector {
	return s.Select(func(b engine.Bucket, rb *engine.RecordBuffer, s *engine.Schema) error {
		f := s.Get(field)
		if f == nil {
			return errors.New("select: field not found")
		}

		if f.Type != engine.Int64Field {
			return errors.New("field incompatible with max, expected an int64")
		}

		var max int64

		var scanner engine.RecordScanner

		c, err := b.Cursor()
		if err != nil {
			return err
		}

		for {
			r, err := c.Next()
			if err != nil {
				return err
			}

			if r == nil {
				break
			}

			scanner.Record = r
			i, err := scanner.GetInt64(field)
			if err != nil {
				return err
			}

			if i > max {
				max = i
			}
		}

		var fb engine.FieldBuffer
		err = fb.SetInt64("max("+field+")", max)
		if err != nil {
			return err
		}

		rb.Add(&fb)

		return nil
	})
}

func (s *Selector) Select(selectorFn func(engine.Bucket, *engine.RecordBuffer, *engine.Schema) error) *Selector {
	s.selectors = append(s.selectors, selectorFn)
	return s
}

type Matcher interface {
	Match(engine.Record) (bool, error)
}

type matcherPipe struct {
	matcher Matcher
}

func (m *matcherPipe) Pipe(b engine.Bucket) (engine.Bucket, error) {
	c, err := b.Cursor()
	if err != nil {
		return nil, err
	}

	var buff engine.RecordBuffer

	for {
		r, err := c.Next()
		if err != nil {
			return nil, err
		}

		if r == nil {
			break
		}

		ok, err := m.matcher.Match(r)
		if err != nil {
			return nil, err
		}

		if !ok {
			continue
		}

		buff.Add(r)
	}

	return &buff, nil
}
