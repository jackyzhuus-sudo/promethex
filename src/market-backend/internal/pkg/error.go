package pkg

import (
	"github.com/go-kratos/kratos/v2/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

var (
	ErrParam    = errors.New(400, "INVALID_PARAM", "param is invalid")
	ErrWait     = errors.New(400, "WAIT", "please wait")
	ErrInternal = errors.New(500, "INTERNAL_ERROR", "internal error")
	ErrNetwork  = errors.New(500, "NETWORK_ERROR", "network error")
)

// FromError try to convert an error to *Error.
// It supports wrapped errors.
func LocalFromError(err error) *errors.Error {
	if err == nil {
		return nil
	}
	if se := new(errors.Error); errors.As(err, &se) {
		return se
	}
	gs, ok := status.FromError(err)
	if !ok {
		return errors.New(500, "", err.Error())
	}
	ret := errors.New(
		int(gs.Code()),
		"",
		gs.Message(),
	)
	for _, detail := range gs.Details() {
		switch d := detail.(type) {
		case *errdetails.ErrorInfo:
			ret.Reason = d.Reason
			return ret.WithMetadata(d.Metadata)
		}
	}
	return ret
}
