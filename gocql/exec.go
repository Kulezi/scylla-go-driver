package gocql

import (
	"context"
	"errors"

	"github.com/kulezi/scylla-go-driver"
	"github.com/kulezi/scylla-go-driver/frame"
	"github.com/kulezi/scylla-go-driver/transport"
)

// SingleHostQueryExecutor allows to quickly execute diagnostic queries while
// connected to only a single node.
// The executor opens only a single connection to a node and does not use
// connection pools.
// Consistency level used is ONE.
// Retry policy is applied, attempts are visible in query metrics but query
// observer is not notified.
type SingleHostQueryExecutor struct {
	conn *transport.Conn
}

// Exec executes the query without returning any rows.
func (e SingleHostQueryExecutor) Exec(stmt string, values ...interface{}) error {
	qStmt := transport.Statement{Content: stmt, Consistency: frame.ONE}
	_, err := e.conn.Query(context.Background(), qStmt, nil)
	return err
}

// Iter executes the query and returns an iterator capable of iterating
// over all results.
func (e SingleHostQueryExecutor) Iter(stmt string, values ...interface{}) *Iter {
	qStmt := transport.Statement{Content: stmt, Consistency: frame.ONE}
	return &Iter{
		it: newSingleHostIter(qStmt, e.conn),
	}
}

func (e SingleHostQueryExecutor) Close() {
	if e.conn != nil {
		e.conn.Close()
	}
}

// NewSingleHostQueryExecutor creates a SingleHostQueryExecutor by connecting
// to one of the hosts specified in the ClusterConfig.
// If ProtoVersion is not specified version 4 is used.
// Caller is responsible for closing the executor after use.
func NewSingleHostQueryExecutor(cfg *ClusterConfig) (e SingleHostQueryExecutor, err error) {
	if len(cfg.Hosts) < 1 {
		return
	}

	var scfg scylla.SessionConfig
	scfg, err = sessionConfigFromGocql(cfg)
	if err != nil {
		return
	}

	host := cfg.Hosts[0]
	var control *transport.Conn
	control, err = transport.OpenConn(context.Background(), host, nil, scfg.ConnConfig)
	if err != nil {
		return
	}

	e = SingleHostQueryExecutor{control}
	return
}

type singleHostIter struct {
	conn        *transport.Conn
	result      transport.QueryResult
	pos         int
	rowCnt      int
	closed      bool
	err         error
	meta        frame.ResultMetadata
	rd          transport.RetryDecider
	stmt        transport.Statement
	pagingState []byte
}

func newSingleHostIter(stmt transport.Statement, conn *transport.Conn) *singleHostIter {
	return &singleHostIter{
		conn: conn,
		stmt: stmt,
	}
}

func (it *singleHostIter) fetch() (transport.QueryResult, error) {
	for {
		res, err := it.conn.Execute(context.Background(), it.stmt, it.result.PagingState)
		if err == nil {
			return res, nil
		} else if err != nil {
			ri := transport.RetryInfo{
				Error:       err,
				Idempotent:  it.stmt.Idempotent,
				Consistency: 1,
			}
			if it.rd.Decide(ri) != transport.RetrySameNode {
				return transport.QueryResult{}, err
			}
		}
	}
}

func (it *singleHostIter) Next() (frame.Row, error) {
	if it.closed {
		return nil, nil
	}

	if it.pos >= it.rowCnt {
		var err error
		it.result, err = it.fetch()
		if err != nil {
			if !errors.Is(err, scylla.ErrNoMoreRows) {
				it.err = err
			}
			return nil, it.Close()
		}

		it.pos = 0
		it.rowCnt = len(it.result.Rows)
	}

	// We probably got a zero-sized last page, retry to be sure
	if it.rowCnt == 0 {
		return it.Next()
	}

	res := it.result.Rows[it.pos]
	it.pos++
	return res, nil
}

func (it *singleHostIter) Close() error {
	if it.closed {
		return it.err
	}
	it.closed = true
	return it.err
}

func (it *singleHostIter) Columns() []frame.ColumnSpec {
	return it.meta.Columns
}

func (it *singleHostIter) NumRows() int {
	return it.rowCnt
}

func (it *singleHostIter) PageState() []byte {
	return it.result.PagingState
}
