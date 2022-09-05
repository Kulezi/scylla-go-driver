package gocql

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/kulezi/scylla-go-driver"
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
type Compressor interface{}
type SimpleRetryPolicy struct {
	NumRetries int
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo TypeInfo
}

type optionWrapper frame.Option

func WrapOption(o *frame.Option) TypeInfo {
	nt := NewNativeType(0x04, Type(o.ID), "")
	switch o.ID {
	case frame.ListID:
		return CollectionType{
			NativeType: nt,
			Elem:       WrapOption(&o.List.Element),
		}
	case frame.SetID:
		return CollectionType{
			NativeType: nt,
			Elem:       WrapOption(&o.Set.Element),
		}
	case frame.MapID:
		return CollectionType{
			NativeType: nt,
			Key:        WrapOption(&o.Map.Key),
			Elem:       WrapOption(&o.Map.Value),
		}
	case frame.UDTID:
		return UDTTypeInfo{
			NativeType: nt,
			KeySpace:   o.UDT.Keyspace,
			Name:       o.UDT.Name,
			Elements:   getUDTFields(o.UDT),
		}
	case frame.CustomID:
		panic("unimplemented")
	default:
		return NewNativeType(0x04, Type(o.ID), "")
	}
}

func getUDTFields(udt *frame.UDTOption) []UDTField {
	res := make([]UDTField, len(udt.FieldNames))
	for i := range res {
		res[i] = UDTField{
			Name: udt.FieldNames[i],
			Type: WrapOption(&udt.FieldTypes[i]),
		}
	}

	return res
}

var ErrNotFound = errors.New("not found")

type Consistency scylla.Consistency

const (
	Any         Consistency = 0x00
	One         Consistency = 0x01
	Two         Consistency = 0x02
	Three       Consistency = 0x03
	Quorum      Consistency = 0x04
	All         Consistency = 0x05
	LocalQuorum Consistency = 0x06
	EachQuorum  Consistency = 0x07
	Serial      Consistency = 0x08
	LocalSerial Consistency = 0x09
	LocalOne    Consistency = 0x0A
)

type SnappyCompressor struct{}

type Authenticator interface{}

type PasswordAuthenticator struct {
	Username, Password string
}

// TypeInfo describes a Cassandra specific data type.
type TypeInfo interface {
	Type() Type
	Version() byte
	Custom() string

	// New creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver
	New() interface{}
}

type Option = frame.Option

type NativeType struct {
	proto  byte
	typ    Type
	custom string // only used for TypeCustom
}

func NewNativeType(proto byte, typ Type, custom string) NativeType {
	return NativeType{proto, typ, custom}
}

func (t NativeType) New() interface{} {
	return reflect.New(goType(t)).Interface()
}

func (s NativeType) Type() Type {
	return s.typ
}

func (s NativeType) Version() byte {
	return s.proto
}

func (s NativeType) Custom() string {
	return s.custom
}

func (s NativeType) String() string {
	switch s.typ {
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", s.typ, s.custom)
	default:
		return s.typ.String()
	}
}

type CollectionType struct {
	NativeType
	Key  TypeInfo // only used for TypeMap
	Elem TypeInfo // only used for TypeMap, TypeList and TypeSet
}

func (t CollectionType) New() interface{} {
	return reflect.New(goType(t)).Interface()
}

func (c CollectionType) String() string {
	switch c.typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", c.typ, c.Key, c.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", c.typ, c.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", c.typ, c.custom)
	default:
		return c.typ.String()
	}
}

type TupleTypeInfo struct {
	NativeType
	Elems []TypeInfo
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.typ))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

func (t TupleTypeInfo) New() interface{} {
	return reflect.New(goType(t)).Interface()
}

type UDTField struct {
	Name string
	Type TypeInfo
}

type UDTTypeInfo struct {
	NativeType
	KeySpace string
	Name     string
	Elements []UDTField
}

func (u UDTTypeInfo) New() interface{} {
	return reflect.New(goType(u)).Interface()
}

func (u UDTTypeInfo) String() string {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "%s.%s{", u.KeySpace, u.Name)
	first := true
	for _, e := range u.Elements {
		if !first {
			fmt.Fprint(buf, ",")
		} else {
			first = false
		}

		fmt.Fprintf(buf, "%s=%v", e.Name, e.Type)
	}
	fmt.Fprint(buf, "}")

	return buf.String()
}
