package gocql

import (
	"context"
	"fmt"

	scylla "github.com/scylladb/scylla-go-driver"
)

type Consistency = scylla.Consistency

var Quorum = scylla.QUORUM

type Session struct {
	session *scylla.Session
}

func NewSession(cfg ClusterConfig) (*Session, error) {
	session, err := scylla.NewSession(context.Background(), sessionConfigFromGocql(&cfg))
	return &Session{session}, err
}

func (s *Session) Query(stmt string, values ...interface{}) *Query {
	q, err := s.session.Prepare(context.Background(), stmt)
	for i, v := range values {
		v, ok := v.(int64)
		if !ok {
			panic("not int64")
		}

		q.BindInt64(i, v)
	}

	return &Query{
		query: q,
		err:   err,
	}
}

func (s *Session) Close() {
	s.session.Close()
}

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

func (q *Query) Iter() Iter {
	return Iter{q.query.Iter(context.Background())}
}

type Scanner interface {
	// Next advances the row pointer to point at the next row, the row is valid until
	// the next call of Next. It returns true if there is a row which is available to be
	// scanned into with Scan.
	// Next must be called before every call to Scan.
	Next() bool

	// Scan copies the current row's columns into dest. If the length of dest does not equal
	// the number of columns returned in the row an error is returned. If an error is encountered
	// when unmarshalling a column into the value in dest an error is returned and the row is invalidated
	// until the next call to Next.
	// Next must be called before calling Scan, if it is not an error is returned.
	Scan(...interface{}) error

	// Err returns the if there was one during iteration that resulted in iteration being unable to complete.
	// Err will also release resources held by the iterator, the Scanner should not used after being called.
	Err() error
}

type Iter struct {
	it scylla.Iter
}
