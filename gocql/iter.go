package gocql

import "github.com/kulezi/scylla-go-driver"

type Iter struct {
	it scylla.Iter
}

func (it *Iter) Columns() []ColumnInfo {
	c := it.it.Columns()
	cols := make([]ColumnInfo, len(c))
	for i, v := range c {
		typ := optionWrap(v.Type)
		cols[i] = ColumnInfo{
			Keyspace: v.Keyspace,
			Table:    v.Table,
			Name:     v.Name,
			TypeInfo: &typ,
		}
	}

	return cols
}
