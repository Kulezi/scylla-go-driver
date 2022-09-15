package gocql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/kulezi/scylla-go-driver"
	"github.com/kulezi/scylla-go-driver/frame"
	"github.com/kulezi/scylla-go-driver/transport"
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

type PoolConfig struct {
	// HostSelectionPolicy sets the policy for selecting which host to use for a
	// given query (default: RoundRobinHostPolicy())
	HostSelectionPolicy HostSelectionPolicy
}

type HostSelectionPolicy interface{}

func TokenAwareHostPolicy(hsp HostSelectionPolicy) HostSelectionPolicy {
	return hsp
}

func RoundRobinHostPolicy() HostSelectionPolicy {
	return transport.NewTokenAwarePolicy("")
}

func DCAwareRoundRobinPolicy(localDC string) HostSelectionPolicy {
	return transport.NewTokenAwarePolicy(localDC)
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

type SslOptions struct {
	*tls.Config

	// CertPath and KeyPath are optional depending on server
	// config, but both fields must be omitted to avoid using a
	// client certificate
	CertPath string
	KeyPath  string
	CaPath   string //optional depending on server config
	// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this
	// on.
	// This option is basically the inverse of tls.Config.InsecureSkipVerify.
	// See InsecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info.
	//
	// See SslOptions documentation to see how EnableHostVerification interacts with the provided tls.Config.
	EnableHostVerification bool
}

func setupTLSConfig(sslOpts *SslOptions) (*tls.Config, error) {
	//  Config.InsecureSkipVerify | EnableHostVerification | Result
	//  Config is nil             | true                   | verify host
	//  Config is nil             | false                  | do not verify host
	//  false                     | false                  | verify host
	//  true                      | false                  | do not verify host
	//  false                     | true                   | verify host
	//  true                      | true                   | verify host
	var tlsConfig *tls.Config
	if sslOpts.Config == nil {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: !sslOpts.EnableHostVerification,
		}
	} else {
		// use clone to avoid race.
		tlsConfig = sslOpts.Config.Clone()
	}

	if tlsConfig.InsecureSkipVerify && sslOpts.EnableHostVerification {
		tlsConfig.InsecureSkipVerify = false
	}

	// ca cert is optional
	if sslOpts.CaPath != "" {
		if tlsConfig.RootCAs == nil {
			tlsConfig.RootCAs = x509.NewCertPool()
		}

		pem, err := ioutil.ReadFile(sslOpts.CaPath)
		if err != nil {
			return nil, fmt.Errorf("connectionpool: unable to open CA certs: %v", err)
		}

		if !tlsConfig.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("connectionpool: failed parsing or CA certs")
		}
	}

	if sslOpts.CertPath != "" || sslOpts.KeyPath != "" {
		mycert, err := tls.LoadX509KeyPair(sslOpts.CertPath, sslOpts.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("connectionpool: unable to load X509 key pair: %v", err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, mycert)
	}

	return tlsConfig, nil
}
