package engine

type Bucket interface {
	Cursor() (Cursor, error)
	Schema() (*Schema, error)
}

type Cursor interface {
	Next() (Record, error)
}

type Engine interface {
	Begin(writable bool) (Transaction, error)
	Close() error
}

type Transaction interface {
	Rollback() error
	Commit() error
	Insert(r Record, path ...string) (key []byte, err error)
	Bucket(path ...string) (Bucket, error)
}

type Pipe interface {
	Pipe(Bucket) (Bucket, error)
}

type Pipeline []Pipe

func (pl Pipeline) Run(b Bucket) (Bucket, error) {
	var err error

	for _, p := range pl {
		b, err = p.Pipe(b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}
