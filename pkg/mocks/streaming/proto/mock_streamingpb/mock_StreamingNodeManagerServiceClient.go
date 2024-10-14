// Code generated by mockery v2.46.0. DO NOT EDIT.

package mock_streamingpb

import (
	context "context"

	grpc "google.golang.org/grpc"

	mock "github.com/stretchr/testify/mock"

	streamingpb "github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
)

// MockStreamingNodeManagerServiceClient is an autogenerated mock type for the StreamingNodeManagerServiceClient type
type MockStreamingNodeManagerServiceClient struct {
	mock.Mock
}

type MockStreamingNodeManagerServiceClient_Expecter struct {
	mock *mock.Mock
}

func (_m *MockStreamingNodeManagerServiceClient) EXPECT() *MockStreamingNodeManagerServiceClient_Expecter {
	return &MockStreamingNodeManagerServiceClient_Expecter{mock: &_m.Mock}
}

// Assign provides a mock function with given fields: ctx, in, opts
func (_m *MockStreamingNodeManagerServiceClient) Assign(ctx context.Context, in *streamingpb.StreamingNodeManagerAssignRequest, opts ...grpc.CallOption) (*streamingpb.StreamingNodeManagerAssignResponse, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, in)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Assign")
	}

	var r0 *streamingpb.StreamingNodeManagerAssignResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *streamingpb.StreamingNodeManagerAssignRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeManagerAssignResponse, error)); ok {
		return rf(ctx, in, opts...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *streamingpb.StreamingNodeManagerAssignRequest, ...grpc.CallOption) *streamingpb.StreamingNodeManagerAssignResponse); ok {
		r0 = rf(ctx, in, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*streamingpb.StreamingNodeManagerAssignResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *streamingpb.StreamingNodeManagerAssignRequest, ...grpc.CallOption) error); ok {
		r1 = rf(ctx, in, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockStreamingNodeManagerServiceClient_Assign_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Assign'
type MockStreamingNodeManagerServiceClient_Assign_Call struct {
	*mock.Call
}

// Assign is a helper method to define mock.On call
//   - ctx context.Context
//   - in *streamingpb.StreamingNodeManagerAssignRequest
//   - opts ...grpc.CallOption
func (_e *MockStreamingNodeManagerServiceClient_Expecter) Assign(ctx interface{}, in interface{}, opts ...interface{}) *MockStreamingNodeManagerServiceClient_Assign_Call {
	return &MockStreamingNodeManagerServiceClient_Assign_Call{Call: _e.mock.On("Assign",
		append([]interface{}{ctx, in}, opts...)...)}
}

func (_c *MockStreamingNodeManagerServiceClient_Assign_Call) Run(run func(ctx context.Context, in *streamingpb.StreamingNodeManagerAssignRequest, opts ...grpc.CallOption)) *MockStreamingNodeManagerServiceClient_Assign_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]grpc.CallOption, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(grpc.CallOption)
			}
		}
		run(args[0].(context.Context), args[1].(*streamingpb.StreamingNodeManagerAssignRequest), variadicArgs...)
	})
	return _c
}

func (_c *MockStreamingNodeManagerServiceClient_Assign_Call) Return(_a0 *streamingpb.StreamingNodeManagerAssignResponse, _a1 error) *MockStreamingNodeManagerServiceClient_Assign_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockStreamingNodeManagerServiceClient_Assign_Call) RunAndReturn(run func(context.Context, *streamingpb.StreamingNodeManagerAssignRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeManagerAssignResponse, error)) *MockStreamingNodeManagerServiceClient_Assign_Call {
	_c.Call.Return(run)
	return _c
}

// CollectStatus provides a mock function with given fields: ctx, in, opts
func (_m *MockStreamingNodeManagerServiceClient) CollectStatus(ctx context.Context, in *streamingpb.StreamingNodeManagerCollectStatusRequest, opts ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, in)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for CollectStatus")
	}

	var r0 *streamingpb.StreamingNodeManagerCollectStatusResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *streamingpb.StreamingNodeManagerCollectStatusRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error)); ok {
		return rf(ctx, in, opts...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *streamingpb.StreamingNodeManagerCollectStatusRequest, ...grpc.CallOption) *streamingpb.StreamingNodeManagerCollectStatusResponse); ok {
		r0 = rf(ctx, in, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*streamingpb.StreamingNodeManagerCollectStatusResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *streamingpb.StreamingNodeManagerCollectStatusRequest, ...grpc.CallOption) error); ok {
		r1 = rf(ctx, in, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockStreamingNodeManagerServiceClient_CollectStatus_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CollectStatus'
type MockStreamingNodeManagerServiceClient_CollectStatus_Call struct {
	*mock.Call
}

// CollectStatus is a helper method to define mock.On call
//   - ctx context.Context
//   - in *streamingpb.StreamingNodeManagerCollectStatusRequest
//   - opts ...grpc.CallOption
func (_e *MockStreamingNodeManagerServiceClient_Expecter) CollectStatus(ctx interface{}, in interface{}, opts ...interface{}) *MockStreamingNodeManagerServiceClient_CollectStatus_Call {
	return &MockStreamingNodeManagerServiceClient_CollectStatus_Call{Call: _e.mock.On("CollectStatus",
		append([]interface{}{ctx, in}, opts...)...)}
}

func (_c *MockStreamingNodeManagerServiceClient_CollectStatus_Call) Run(run func(ctx context.Context, in *streamingpb.StreamingNodeManagerCollectStatusRequest, opts ...grpc.CallOption)) *MockStreamingNodeManagerServiceClient_CollectStatus_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]grpc.CallOption, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(grpc.CallOption)
			}
		}
		run(args[0].(context.Context), args[1].(*streamingpb.StreamingNodeManagerCollectStatusRequest), variadicArgs...)
	})
	return _c
}

func (_c *MockStreamingNodeManagerServiceClient_CollectStatus_Call) Return(_a0 *streamingpb.StreamingNodeManagerCollectStatusResponse, _a1 error) *MockStreamingNodeManagerServiceClient_CollectStatus_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockStreamingNodeManagerServiceClient_CollectStatus_Call) RunAndReturn(run func(context.Context, *streamingpb.StreamingNodeManagerCollectStatusRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error)) *MockStreamingNodeManagerServiceClient_CollectStatus_Call {
	_c.Call.Return(run)
	return _c
}

// Remove provides a mock function with given fields: ctx, in, opts
func (_m *MockStreamingNodeManagerServiceClient) Remove(ctx context.Context, in *streamingpb.StreamingNodeManagerRemoveRequest, opts ...grpc.CallOption) (*streamingpb.StreamingNodeManagerRemoveResponse, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, in)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Remove")
	}

	var r0 *streamingpb.StreamingNodeManagerRemoveResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *streamingpb.StreamingNodeManagerRemoveRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeManagerRemoveResponse, error)); ok {
		return rf(ctx, in, opts...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *streamingpb.StreamingNodeManagerRemoveRequest, ...grpc.CallOption) *streamingpb.StreamingNodeManagerRemoveResponse); ok {
		r0 = rf(ctx, in, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*streamingpb.StreamingNodeManagerRemoveResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *streamingpb.StreamingNodeManagerRemoveRequest, ...grpc.CallOption) error); ok {
		r1 = rf(ctx, in, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockStreamingNodeManagerServiceClient_Remove_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Remove'
type MockStreamingNodeManagerServiceClient_Remove_Call struct {
	*mock.Call
}

// Remove is a helper method to define mock.On call
//   - ctx context.Context
//   - in *streamingpb.StreamingNodeManagerRemoveRequest
//   - opts ...grpc.CallOption
func (_e *MockStreamingNodeManagerServiceClient_Expecter) Remove(ctx interface{}, in interface{}, opts ...interface{}) *MockStreamingNodeManagerServiceClient_Remove_Call {
	return &MockStreamingNodeManagerServiceClient_Remove_Call{Call: _e.mock.On("Remove",
		append([]interface{}{ctx, in}, opts...)...)}
}

func (_c *MockStreamingNodeManagerServiceClient_Remove_Call) Run(run func(ctx context.Context, in *streamingpb.StreamingNodeManagerRemoveRequest, opts ...grpc.CallOption)) *MockStreamingNodeManagerServiceClient_Remove_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]grpc.CallOption, len(args)-2)
		for i, a := range args[2:] {
			if a != nil {
				variadicArgs[i] = a.(grpc.CallOption)
			}
		}
		run(args[0].(context.Context), args[1].(*streamingpb.StreamingNodeManagerRemoveRequest), variadicArgs...)
	})
	return _c
}

func (_c *MockStreamingNodeManagerServiceClient_Remove_Call) Return(_a0 *streamingpb.StreamingNodeManagerRemoveResponse, _a1 error) *MockStreamingNodeManagerServiceClient_Remove_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockStreamingNodeManagerServiceClient_Remove_Call) RunAndReturn(run func(context.Context, *streamingpb.StreamingNodeManagerRemoveRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeManagerRemoveResponse, error)) *MockStreamingNodeManagerServiceClient_Remove_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockStreamingNodeManagerServiceClient creates a new instance of MockStreamingNodeManagerServiceClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockStreamingNodeManagerServiceClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockStreamingNodeManagerServiceClient {
	mock := &MockStreamingNodeManagerServiceClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
