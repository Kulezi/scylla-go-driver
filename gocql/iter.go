package gocql

import (
	"fmt"

	"github.com/kulezi/scylla-go-driver"
	"github.com/kulezi/scylla-go-driver/frame"
)

type Iter struct {
	it  scylla.Iter
	err error
}

func (it *Iter) Columns() []ColumnInfo {
	c := it.it.Columns()
	cols := make([]ColumnInfo, len(c))
	for i, v := range c {
		typ := WrapOption(&v.Type)
		cols[i] = ColumnInfo{
			Keyspace: v.Keyspace,
			Table:    v.Table,
			Name:     v.Name,
			TypeInfo: typ,
		}
	}

	return cols
}

func (it *Iter) NumRows() int {
	return it.it.NumRows()
}

func (it *Iter) Close() error {
	return it.it.Close()
}

func (it *Iter) Scan(dest ...interface{}) bool {
	if it.err != nil {
		return false
	}

	var r frame.Row
	r, it.err = it.it.Next()
	if it.err != nil {
		return false
	}

	if len(dest) != len(r) {
		it.err = fmt.Errorf("expected %d columns, got %d", len(dest), len(r))
		return false
	}

	for i := range dest {
		it.err = Unmarshal(WrapOption(r[i].Type), r[i].Value, dest[i])
		if it.err != nil {
			return false
		}
	}

	return true
}

func (it *Iter) PageState() []byte {
	return nil
}
