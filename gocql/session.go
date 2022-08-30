package gocql

import (
	"context"
	"time"

	"github.com/kulezi/scylla-go-driver"
)

type Consistency = scylla.Consistency

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

func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	// TODO: wait for actual schema agreement.
	time.Sleep(time.Second)
	return nil
}
