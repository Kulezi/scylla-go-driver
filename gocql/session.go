package gocql

import (
	"context"
	"time"

	"github.com/kulezi/scylla-go-driver"
)

type Session struct {
	session *scylla.Session
	cfg     scylla.SessionConfig
}

func NewSession(cfg ClusterConfig) (*Session, error) {
	scfg, err := sessionConfigFromGocql(&cfg)
	if err != nil {
		return nil, err
	}

	session, err := scylla.NewSession(context.Background(), scfg)
	return &Session{
		session: session,
		cfg:     scfg,
	}, err
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

func (s *Session) Closed() bool {
	return s.session.Closed()
}

func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	// TODO: wait for actual schema agreement.
	time.Sleep(time.Second)
	return nil
}
