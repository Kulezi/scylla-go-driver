package request

import (
	"github.com/kulezi/scylla-go-driver/frame"
)

var _ frame.Request = (*Options)(nil)

// Options spec: https://github.com/apache/cassandra/blob/adcff3f630c0d07d1ba33bf23fcb11a6db1b9af1/doc/native_protocol_v4.spec#L330
type Options struct{}

func (*Options) WriteTo(_ *frame.Buffer) {}

func (*Options) OpCode() frame.OpCode {
	return frame.OpOptions
}
