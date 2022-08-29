package scylla

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/kulezi/scylla-go-driver/frame"
	"github.com/kulezi/scylla-go-driver/transport"
)

type Query struct {
	session   *Session
	stmt      transport.Statement
	buf       frame.Buffer
	exec      func(context.Context, *transport.Conn, transport.Statement, frame.Bytes) (transport.QueryResult, error)
	asyncExec func(context.Context, *transport.Conn, transport.Statement, frame.Bytes, transport.ResponseHandler)
	res       []transport.ResponseHandler

	err []error
}

func (q *Query) Exec(ctx context.Context) (Result, error) {
	if q.err != nil {
		return Result{}, fmt.Errorf("query can't be executed: %w", q.err)
	}
	conn, err := q.pickConn()
	if err != nil {
		return Result{}, err
	}

	res, err := q.exec(ctx, conn, q.stmt, nil)
	return Result(res), err
}

func (q *Query) pickConn() (*transport.Conn, error) {
	token, tokenAware := q.token()
	info, err := q.info(token, tokenAware)
	if err != nil {
		return nil, err
	}
	n := q.session.policy.Node(info, 0)

	var conn *transport.Conn
	if tokenAware {
		conn = n.Conn(token)
	} else {
		conn = n.LeastBusyConn()
	}
	if conn == nil {
		return nil, errNoConnection
	}

	return conn, nil
}

func (q *Query) AsyncExec(ctx context.Context) {
	stmt := q.stmt.Clone()

	conn, err := q.pickConn()
	if err != nil {
		q.res = append(q.res, transport.MakeResponseHandlerWithError(err))
		return
	}

	h := transport.MakeResponseHandler()
	q.res = append(q.res, h)
	q.asyncExec(ctx, conn, stmt, nil, h)
}

var ErrNoQueryResults = fmt.Errorf("no query results to be fetched")

// Fetch returns results in the same order they were queried.
func (q *Query) Fetch() (Result, error) {
	if len(q.res) == 0 {
		return Result{}, ErrNoQueryResults
	}

	h := q.res[0]
	q.res = q.res[1:]

	resp := <-h
	if resp.Err != nil {
		return Result{}, resp.Err
	}

	res, err := transport.MakeQueryResult(resp.Response, q.stmt.Metadata)
	return Result(res), err
}

// https://github.com/kulezi/scylla/blob/40adf38915b6d8f5314c621a94d694d172360833/compound_compat.hh#L33-L47
func (q *Query) token() (transport.Token, bool) {
	if q.stmt.PkCnt == 0 {
		return 0, false
	}

	q.buf.Reset()
	if q.stmt.PkCnt == 1 {
		return transport.MurmurToken(q.stmt.Values[q.stmt.PkIndexes[0]].Bytes), true
	}
	for _, idx := range q.stmt.PkIndexes {
		size := q.stmt.Values[idx].N
		q.buf.WriteShort(frame.Short(size))
		q.buf.Write(q.stmt.Values[idx].Bytes)
		q.buf.WriteByte(0)
	}

	return transport.MurmurToken(q.buf.Bytes()), true
}

func (q *Query) info(token transport.Token, tokenAware bool) (transport.QueryInfo, error) {
	if tokenAware {
		// TODO: Will the driver support using different keyspaces than default?
		info, err := q.session.cluster.NewTokenAwareQueryInfo(token, "")
		return info, err
	}

	return q.session.cluster.NewQueryInfo(), nil
}

func (q *Query) checkBounds(pos int) error {
	if q.stmt.Metadata != nil {
		if pos < 0 || pos >= len(q.stmt.Values) {
			return fmt.Errorf("no bind marker with position %d", pos)
		}

		return nil
	}

	for i := len(q.stmt.Values); i <= pos; i++ {
		q.stmt.Values = append(q.stmt.Values, frame.Value{})
	}
	return nil
}

// BindAny allows binding any value to the bind marker at given pos in query,
// it shouldn't be used on non-prepared queries, as it will always result in query execution error later.
func (q *Query) BindAny(pos int, x any) *Query {
	if q.stmt.Metadata == nil {
		q.err = append(q.err, fmt.Errorf("binding any to unprepared queries is not supported"))
		return q
	}
	if err := q.checkBounds(pos); err != nil {
		q.err = append(q.err, err)
		return q
	}

	var err error

	typ := q.stmt.Values[pos].Type
	if typ.ID == frame.ListID {
		typ := gocql.CollectionType{
			NativeType: gocql.NewNativeType(0x04, typ.Type(), ""),
			Elem:       &q.stmt.Values[pos].Type.List.Element,
		}
		q.stmt.Values[pos].Bytes, err = gocql.Marshal(typ, x)
	} else {
		q.stmt.Values[pos].Bytes, err = gocql.Marshal(q.stmt.Values[pos].Type, x)
	}

	if err != nil {
		q.err = append(q.err, err)
		return q
	}
	q.stmt.Values[pos].N = int32(len(q.stmt.Values[pos].Bytes))

	return q
}

func (q *Query) BindInt64(pos int, v int64) *Query {
	p := &q.stmt.Values[pos]
	if p.N == 0 {
		p.N = 8
		p.Bytes = make([]byte, 8)
	}

	p.Bytes[0] = byte(v >> 56)
	p.Bytes[1] = byte(v >> 48)
	p.Bytes[2] = byte(v >> 40)
	p.Bytes[3] = byte(v >> 32)
	p.Bytes[4] = byte(v >> 24)
	p.Bytes[5] = byte(v >> 16)
	p.Bytes[6] = byte(v >> 8)
	p.Bytes[7] = byte(v)

	return q
}

func (q *Query) SetPageSize(v int32) {
	q.stmt.PageSize = v
}

func (q *Query) PageSize() int32 {
	return q.stmt.PageSize
}

func (q *Query) SetCompression(v bool) {
	q.stmt.Compression = v
}

func (q *Query) Compression() bool {
	return q.stmt.Compression
}

type Result transport.QueryResult

func (q *Query) Iter(ctx context.Context) Iter {
	stmt := q.stmt.Clone()
	it := Iter{
		requestCh: make(chan struct{}, 1),
		nextCh:    make(chan transport.QueryResult),
		errCh:     make(chan error, 1),

		meta: *stmt.Metadata,
	}

	conn, err := q.pickConn()
	if err != nil {
		it.errCh <- err
		return it
	}

	worker := iterWorker{
		stmt:      stmt,
		conn:      conn,
		queryExec: q.exec,
		requestCh: it.requestCh,
		nextCh:    it.nextCh,
		errCh:     it.errCh,
	}

	it.requestCh <- struct{}{}
	go worker.loop(ctx)
	return it
}

type Iter struct {
	result transport.QueryResult
	pos    int
	rowCnt int

	requestCh chan struct{}
	nextCh    chan transport.QueryResult
	errCh     chan error
	closed    bool

	meta frame.ResultMetadata
}

var (
	ErrClosedIter = fmt.Errorf("iter is closed")
	ErrNoMoreRows = fmt.Errorf("no more rows left")
)

func (it *Iter) Next() (frame.Row, error) {
	if it.closed {
		return nil, ErrClosedIter
	}

	if it.pos >= it.rowCnt {
		select {
		case r := <-it.nextCh:
			it.result = r
		case err := <-it.errCh:
			it.Close()
			return nil, err
		}

		it.pos = 0
		it.rowCnt = len(it.result.Rows)
		it.requestCh <- struct{}{}
	}

	// We probably got a zero-sized last page, retry to be sure
	if it.rowCnt == 0 {
		return it.Next()
	}

	res := it.result.Rows[it.pos]
	it.pos++
	return res, nil
}

func (it *Iter) Close() {
	if it.closed {
		return
	}
	it.closed = true
	close(it.requestCh)
}

func (it *Iter) Columns() []frame.ColumnSpec {
	return it.meta.Columns
}

type iterWorker struct {
	stmt        transport.Statement
	conn        *transport.Conn
	pagingState []byte
	queryExec   func(context.Context, *transport.Conn, transport.Statement, frame.Bytes) (transport.QueryResult, error)

	requestCh chan struct{}
	nextCh    chan transport.QueryResult
	errCh     chan error
}

func (w *iterWorker) loop(ctx context.Context) {
	for {
		_, ok := <-w.requestCh
		if !ok {
			return
		}

		res, err := w.queryExec(ctx, w.conn, w.stmt, w.pagingState)
		if err != nil {
			w.errCh <- err
			return
		}
		w.pagingState = res.PagingState
		w.nextCh <- res

		if !res.HasMorePages {
			w.errCh <- ErrNoMoreRows
			return
		}
	}
}
