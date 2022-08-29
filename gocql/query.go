package gocql

import (
	"context"
	"fmt"

	"github.com/kulezi/scylla-go-driver"
)

type Query struct {
	ctx   context.Context
	query scylla.Query
	err   error
}

func (q *Query) Bind(values ...interface{}) *Query {
	for i, v := range values {
		q.query.BindAny(i, v)
	}

	return q
}

func (q *Query) Exec() error {
	_, err := q.query.Exec(q.ctx)
	return err
}

func (q *Query) Scan(values ...interface{}) error {
	res, err := q.query.Exec(q.ctx)
	if err != nil {
		return err
	}

	if len(res.Rows[0]) != len(values) {
		return fmt.Errorf("column count mismatch expected %d, got %d", len(values), len(res.Rows))
	}

	for i, v := range res.Rows[0] {
		if err := v.Unmarshal(values[i]); err != nil {
			return err
		}
	}

	return nil
}

func (q *Query) Iter() *Iter {
	return &Iter{q.query.Iter(q.ctx)}
}

func (q *Query) Release() {
	// TODO: does this need to do anything, new driver doesn't have a pool of queries.
}

func (q *Query) WithContext(ctx context.Context) *Query {
	q.ctx = ctx
	return q
}
