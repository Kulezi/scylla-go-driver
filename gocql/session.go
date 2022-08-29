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
		v, ok := v.(int64)
		if !ok {
			panic("not int64")
		}

		q.query.BindInt64(i, v)
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
		dst, ok := values[i].(*int64)
		if !ok {
			return fmt.Errorf("not int64")
		}

		*dst, err = v.AsInt64()
		if err != nil {
			return err
		}
	}

	return nil
}
