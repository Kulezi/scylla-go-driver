package gocql

import (
	"context"

	"github.com/kulezi/scylla-go-driver"
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
		ctx:   context.Background(),
		query: q,
		err:   err,
	}
}

func (s *Session) Close() {
	s.session.Close()
}
