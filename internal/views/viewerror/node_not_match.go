package viewerror

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NewGRPCStatusFromNodeNotMatch preserves the routing error produced by the
// ServerID interceptor. Other errors retain their existing gRPC representation.
func NewGRPCStatusFromNodeNotMatch(err error) *status.Status {
	if merr.Code(err) != merr.Code(merr.ErrNodeNotMatch) {
		return status.Convert(err)
	}

	// Retrying the same target cannot succeed. The application must observe a
	// newer channel assignment before retrying, rather than gRPC retrying it.
	st := status.New(codes.FailedPrecondition, err.Error())
	withDetails, detailErr := st.WithDetails(merr.Status(err))
	if detailErr != nil {
		return st
	}
	return withDetails
}

// IsNodeNotMatch recognizes the exact routing code, locally or in gRPC details.
// Error messages and generic transport codes do not identify this condition.
func IsNodeNotMatch(err error) bool {
	if err == nil {
		return false
	}
	if merr.IsMilvusError(err) {
		return merr.Code(err) == merr.Code(merr.ErrNodeNotMatch)
	}
	rpcStatus, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, detail := range rpcStatus.Details() {
		if detail, ok := detail.(*commonpb.Status); ok && detail.GetCode() == merr.Code(merr.ErrNodeNotMatch) {
			return true
		}
	}
	return false
}
