package gocql

import (
	"context"
	"fmt"

	"github.com/kulezi/scylla-go-driver"
	"github.com/kulezi/scylla-go-driver/frame"
)

type Query struct {
	ctx   context.Context
	query scylla.Query
	err   error
}

type anyWrapper struct {
	val any
}

func (w anyWrapper) Serialize(o *frame.Option) (n int32, bytes []byte, err error) {
	ti := WrapOption(o)
	bytes, err = Marshal(ti, w.val)
	if bytes == nil {
		n = -1
	} else {
		n = int32(len(bytes))
	}
	return
}

func (q *Query) Bind(values ...interface{}) *Query {
	for i, v := range values {
		q.query.Bind(i, anyWrapper{v})
	}
	return q
}

func (q *Query) Exec() error {
	_, err := q.query.Exec(q.ctx)
	return err
}

func unmarshalCqlValue(c frame.CqlValue, dst interface{}) error {
	return Unmarshal(WrapOption(c.Type), c.Value, dst)
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
		if err := unmarshalCqlValue(v, values[i]); err != nil {
			return err
		}
	}

	return nil
}

func (q *Query) Iter() *Iter {
	return &Iter{it: q.query.Iter(q.ctx)}
}

func (q *Query) Release() {
	// TODO: does this need to do anything, new driver doesn't have a pool of queries.
}

func (q *Query) WithContext(ctx context.Context) *Query {
	q.ctx = ctx
	return q
}

func (q *Query) Consistency(c Consistency) *Query {
	panic("unimplemented")
}

// CustomPayload sets the custom payload level for this query.
func (q *Query) CustomPayload(customPayload map[string][]byte) *Query {
	panic("unimplemented")
}

// Trace enables tracing of this query. Look at the documentation of the
// Tracer interface to learn more about tracing.
func (q *Query) Trace(trace Tracer) *Query {
	panic("unimplemented")
}

// Observer enables query-level observer on this query.
// The provided observer will be called every time this query is executed.
func (q *Query) Observer(observer QueryObserver) *Query {
	panic("unimplemented")
}

func (q *Query) PageSize(n int) *Query {
	q.query.SetPageSize(int32(n))
	return q
}

func (q *Query) DefaultTimestamp(enable bool) *Query {
	panic("unimplemented")
}

func (q *Query) WithTimestamp(timestamp int64) *Query {
	panic("unimplemented")
}

func (q *Query) RoutingKey(routingKey []byte) *Query {
	panic("unimplemented")
}

func (q *Query) Prefetch(p float64) *Query {
	panic("unimplemented")
}

func (q *Query) RetryPolicy(r RetryPolicy) *Query {
	panic("unimplemented")
}

func (q *Query) SetSpeculativeExecutionPolicy(sp SpeculativeExecutionPolicy) *Query {
	panic("unimplemented")
}

func (q *Query) Idempotent(value bool) *Query {
	panic("unimplemented")
	// q.query.SetIdempotent(value)
}

func (q *Query) SerialConsistency(cons SerialConsistency) *Query {
	panic("unimplemented")
}

func (q *Query) PageState(state []byte) *Query {
	q.query.SetPageState(state)
	return q
}

func (q *Query) NoSkipMetadata() *Query {
	panic("unimplemented")
}
