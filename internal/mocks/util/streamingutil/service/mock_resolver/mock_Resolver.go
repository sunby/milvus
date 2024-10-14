// Code generated by mockery v2.46.0. DO NOT EDIT.

package mock_resolver

import (
	context "context"

	discoverer "github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	mock "github.com/stretchr/testify/mock"
)

// MockResolver is an autogenerated mock type for the Resolver type
type MockResolver struct {
	mock.Mock
}

type MockResolver_Expecter struct {
	mock *mock.Mock
}

func (_m *MockResolver) EXPECT() *MockResolver_Expecter {
	return &MockResolver_Expecter{mock: &_m.Mock}
}

// GetLatestState provides a mock function with given fields:
func (_m *MockResolver) GetLatestState() discoverer.VersionedState {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetLatestState")
	}

	var r0 discoverer.VersionedState
	if rf, ok := ret.Get(0).(func() discoverer.VersionedState); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(discoverer.VersionedState)
	}

	return r0
}

// MockResolver_GetLatestState_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLatestState'
type MockResolver_GetLatestState_Call struct {
	*mock.Call
}

// GetLatestState is a helper method to define mock.On call
func (_e *MockResolver_Expecter) GetLatestState() *MockResolver_GetLatestState_Call {
	return &MockResolver_GetLatestState_Call{Call: _e.mock.On("GetLatestState")}
}

func (_c *MockResolver_GetLatestState_Call) Run(run func()) *MockResolver_GetLatestState_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockResolver_GetLatestState_Call) Return(_a0 discoverer.VersionedState) *MockResolver_GetLatestState_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockResolver_GetLatestState_Call) RunAndReturn(run func() discoverer.VersionedState) *MockResolver_GetLatestState_Call {
	_c.Call.Return(run)
	return _c
}

// Watch provides a mock function with given fields: ctx, cb
func (_m *MockResolver) Watch(ctx context.Context, cb func(discoverer.VersionedState) error) error {
	ret := _m.Called(ctx, cb)

	if len(ret) == 0 {
		panic("no return value specified for Watch")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, func(discoverer.VersionedState) error) error); ok {
		r0 = rf(ctx, cb)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockResolver_Watch_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Watch'
type MockResolver_Watch_Call struct {
	*mock.Call
}

// Watch is a helper method to define mock.On call
//   - ctx context.Context
//   - cb func(discoverer.VersionedState) error
func (_e *MockResolver_Expecter) Watch(ctx interface{}, cb interface{}) *MockResolver_Watch_Call {
	return &MockResolver_Watch_Call{Call: _e.mock.On("Watch", ctx, cb)}
}

func (_c *MockResolver_Watch_Call) Run(run func(ctx context.Context, cb func(discoverer.VersionedState) error)) *MockResolver_Watch_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(func(discoverer.VersionedState) error))
	})
	return _c
}

func (_c *MockResolver_Watch_Call) Return(_a0 error) *MockResolver_Watch_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockResolver_Watch_Call) RunAndReturn(run func(context.Context, func(discoverer.VersionedState) error) error) *MockResolver_Watch_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockResolver creates a new instance of MockResolver. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockResolver(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockResolver {
	mock := &MockResolver{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
