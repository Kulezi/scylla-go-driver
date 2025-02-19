package transport

import (
	"fmt"

	"github.com/kulezi/scylla-go-driver/frame"
	. "github.com/kulezi/scylla-go-driver/frame/response"
)

// responseAsError returns either IoError or some error defined in response.error.
func responseAsError(res frame.Response) error {
	if v, ok := res.(CodedError); ok {
		return v
	}
	return fmt.Errorf("unexpected response %T, %+v", res, res)
}
