package gocql

import (
	"errors"
	"reflect"

	"github.com/kulezi/scylla-go-driver/frame"
)

type unsetColumn struct{}

// UnsetValue represents a value used in a query binding that will be ignored by Cassandra.
//
// By setting a field to the unset value Cassandra will ignore the write completely.
// The main advantage is the ability to keep the same prepared statement even when you don't
// want to update some fields, where before you needed to make another prepared statement.
//
// UnsetValue is only available when using the version 4 of the protocol.
var UnsetValue = unsetColumn{}

const (
	protoDirectionMask = 0x80
	protoVersionMask   = 0x7F
	protoVersion1      = 0x01
	protoVersion2      = 0x02
	protoVersion3      = 0x03
	protoVersion4      = 0x04
	protoVersion5      = 0x05
)

type Duration struct {
	Months      int32
	Days        int32
	Nanoseconds int64
}

type RetryPolicy interface{} // TODO: use retry policy
type SpeculativeExecutionPolicy interface{}
type SerialConsistency interface{}
type QueryObserver interface{}
type Tracer interface{}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo TypeInfo
}

type optionWrap frame.Option

func (o *optionWrap) Type() Type {
	return Type(o.ID)
}

func (o *optionWrap) Version() byte {
	return frame.CQLv4
}

func (o *optionWrap) Custom() string {
	return ""
}

func (o *optionWrap) New() interface{} {
	return nil
}

func (o *optionWrap) NewWithError() (interface{}, error) {
	typ := goType(o)
	return reflect.New(typ).Interface(), nil
}

var ErrNotFound = errors.New("not found")
