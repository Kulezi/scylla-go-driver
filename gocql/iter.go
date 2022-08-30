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

func (it *Iter) NumRows() int {
	return it.it.NumRows()
}

func (it *Iter) Close() error {
	return it.it.Close()
}

func (it *Iter) Scan(dest ...interface{}) bool {
	return it.it.Scan(dest...)
}

func (it *Iter) PageState() []byte {
	return nil
}
