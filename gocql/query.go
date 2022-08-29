package gocql

import (
	"context"
	"fmt"

	"github.com/kulezi/scylla-go-driver"
)

type Query struct {
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
	_, err := q.query.Exec(context.Background())
	return err
}

func (q *Query) Scan(values ...interface{}) error {
	res, err := q.query.Exec(context.Background())
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
	return &Iter{q.query.Iter(context.Background())}
}

func (q *Query) Release() {
	// TODO: does this need to do anything, new driver doesn't have a pool of queries.
}
