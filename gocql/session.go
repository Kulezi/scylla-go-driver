package gocql

import (
	"context"
	"time"

	"github.com/kulezi/scylla-go-driver"
)

type Session struct {
	session *scylla.Session
}

func NewSession(cfg ClusterConfig) (*Session, error) {
	session, err := scylla.NewSession(context.Background(), sessionConfigFromGocql(&cfg))
	return &Session{session}, err
}

func (s *Session) Query(stmt string, values ...interface{}) *Query {
	return &Query{
		ctx:    context.Background(),
		query:  s.session.Query(stmt),
		values: values,
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
