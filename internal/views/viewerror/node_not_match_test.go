package viewerror

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestNodeNotMatchStatusRoundTrip(t *testing.T) {
	original := merr.Wrap(merr.WrapErrNodeNotMatch(1682, 1683), "routing query plan")
	rpcStatus := NewGRPCStatusFromNodeNotMatch(original)
	require.Equal(t, codes.FailedPrecondition, rpcStatus.Code())
	require.Equal(t, original.Error(), rpcStatus.Message())
	require.Len(t, rpcStatus.Details(), 1)
	wireStatus, ok := rpcStatus.Details()[0].(*commonpb.Status)
	require.True(t, ok)
	require.EqualValues(t, 904, wireStatus.GetCode())
	require.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, wireStatus.GetErrorCode())
	require.Equal(t, original.Error(), wireStatus.GetReason())
	require.False(t, wireStatus.GetRetriable())
	require.ErrorIs(t, merr.Error(wireStatus), merr.ErrNodeNotMatch)

	converted := ConvertViewError("QueryPlanService.GetQueryPlan", rpcStatus.Err())
	require.IsType(t, &ViewClientStatus{}, converted)
	require.True(t, IsNodeNotMatch(converted))
	require.True(t, IsNodeNotMatch(merr.Wrap(converted, "request failed")))
	require.Nil(t, converted.(*ViewClientStatus).TryIntoViewError())
}

func TestIsNodeNotMatch(t *testing.T) {
	nodeErr := merr.WrapErrNodeNotMatch(1682, 1683)
	wireErr := NewGRPCStatusFromNodeNotMatch(nodeErr).Err()
	crossClusterStatus, err := status.New(codes.FailedPrecondition, "").WithDetails(
		merr.Status(merr.WrapErrServiceCrossClusterRouting("one", "two")))
	require.NoError(t, err)
	otherStatus, err := status.New(codes.FailedPrecondition, "").WithDetails(&commonpb.Status{
		Code:      merr.Code(merr.ErrServiceInternal),
		ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
		Reason:    nodeErr.Error(),
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{name: "local", err: nodeErr, want: true},
		{name: "wrapped local", err: merr.Wrap(nodeErr, "routing"), want: true},
		{name: "wire", err: wireErr, want: true},
		{name: "wrapped wire", err: merr.Wrap(wireErr, "routing"), want: true},
		{name: "converted wire", err: ConvertViewError("method", wireErr), want: true},
		{name: "relabeled local", err: merr.WrapErrServiceInternalErr(nodeErr, "internal failure")},
		{name: "relabeled wire", err: merr.WrapErrServiceInternalErr(wireErr, "internal failure")},
		{name: "plain message", err: errors.New(nodeErr.Error())},
		{name: "unknown message", err: status.Error(codes.Unknown, nodeErr.Error())},
		{name: "failed precondition without details", err: status.Error(codes.FailedPrecondition, nodeErr.Error())},
		{name: "view error", err: NewGRPCStatusFromViewError(NewNotPrimaryError(nodeErr.Error())).Err()},
		{name: "other numeric code", err: otherStatus.Err()},
		{name: "cross cluster", err: crossClusterStatus.Err()},
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, IsNodeNotMatch(test.err))
		})
	}
}
