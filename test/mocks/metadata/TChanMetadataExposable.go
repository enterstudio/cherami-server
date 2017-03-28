// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metadata

import "github.com/uber/cherami-thrift/.generated/go/shared"
import "github.com/uber/cherami-thrift/.generated/go/metadata"
import "github.com/stretchr/testify/mock"

import "github.com/uber/tchannel-go/thrift"

// TChanMetadataExposable is an autogenerated mock type for the TChanMetadataExposable type
type TChanMetadataExposable struct {
	mock.Mock
}

// ListEntityOps provides a mock function with given fields: ctx, listRequest
func (_m *TChanMetadataExposable) ListEntityOps(ctx thrift.Context, listRequest *metadata.ListEntityOpsRequest) (*metadata.ListEntityOpsResult_, error) {
	ret := _m.Called(ctx, listRequest)

	var r0 *metadata.ListEntityOpsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ListEntityOpsRequest) *metadata.ListEntityOpsResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ListEntityOpsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ListEntityOpsRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateServiceConfig provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) CreateServiceConfig(ctx thrift.Context, request *metadata.CreateServiceConfigRequest) error {
	ret := _m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.CreateServiceConfigRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteServiceConfig provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) DeleteServiceConfig(ctx thrift.Context, request *metadata.DeleteServiceConfigRequest) error {
	ret := _m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.DeleteServiceConfigRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// HostAddrToUUID provides a mock function with given fields: ctx, hostAddr
func (_m *TChanMetadataExposable) HostAddrToUUID(ctx thrift.Context, hostAddr string) (string, error) {
	ret := _m.Called(ctx, hostAddr)

	var r0 string
	if rf, ok := ret.Get(0).(func(thrift.Context, string) string); ok {
		r0 = rf(ctx, hostAddr)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, string) error); ok {
		r1 = rf(ctx, hostAddr)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListAllConsumerGroups provides a mock function with given fields: ctx, listRequest
func (_m *TChanMetadataExposable) ListAllConsumerGroups(ctx thrift.Context, listRequest *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {
	ret := _m.Called(ctx, listRequest)

	var r0 *shared.ListConsumerGroupResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListConsumerGroupRequest) *shared.ListConsumerGroupResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListConsumerGroupResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListConsumerGroupRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListConsumerGroups provides a mock function with given fields: ctx, listRequest
func (_m *TChanMetadataExposable) ListConsumerGroups(ctx thrift.Context, listRequest *shared.ListConsumerGroupRequest) (*shared.ListConsumerGroupResult_, error) {
	ret := _m.Called(ctx, listRequest)

	var r0 *shared.ListConsumerGroupResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListConsumerGroupRequest) *shared.ListConsumerGroupResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListConsumerGroupResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListConsumerGroupRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListDestinations provides a mock function with given fields: ctx, listRequest
func (_m *TChanMetadataExposable) ListDestinations(ctx thrift.Context, listRequest *shared.ListDestinationsRequest) (*shared.ListDestinationsResult_, error) {
	ret := _m.Called(ctx, listRequest)

	var r0 *shared.ListDestinationsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListDestinationsRequest) *shared.ListDestinationsResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListDestinationsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListDestinationsRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListDestinationsByUUID provides a mock function with given fields: ctx, listRequest
func (_m *TChanMetadataExposable) ListDestinationsByUUID(ctx thrift.Context, listRequest *shared.ListDestinationsByUUIDRequest) (*shared.ListDestinationsResult_, error) {
	ret := _m.Called(ctx, listRequest)

	var r0 *shared.ListDestinationsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListDestinationsByUUIDRequest) *shared.ListDestinationsResult_); ok {
		r0 = rf(ctx, listRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListDestinationsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListDestinationsByUUIDRequest) error); ok {
		r1 = rf(ctx, listRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListExtentsStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListExtentsStats(ctx thrift.Context, request *shared.ListExtentsStatsRequest) (*shared.ListExtentsStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *shared.ListExtentsStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListExtentsStatsRequest) *shared.ListExtentsStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListExtentsStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListExtentsStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListHosts provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListHosts(ctx thrift.Context, request *metadata.ListHostsRequest) (*metadata.ListHostsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ListHostsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ListHostsRequest) *metadata.ListHostsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ListHostsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ListHostsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListInputHostExtentsStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListInputHostExtentsStats(ctx thrift.Context, request *metadata.ListInputHostExtentsStatsRequest) (*metadata.ListInputHostExtentsStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ListInputHostExtentsStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ListInputHostExtentsStatsRequest) *metadata.ListInputHostExtentsStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ListInputHostExtentsStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ListInputHostExtentsStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListStoreExtentsStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListStoreExtentsStats(ctx thrift.Context, request *metadata.ListStoreExtentsStatsRequest) (*metadata.ListStoreExtentsStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ListStoreExtentsStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ListStoreExtentsStatsRequest) *metadata.ListStoreExtentsStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ListStoreExtentsStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ListStoreExtentsStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroup provides a mock function with given fields: ctx, getRequest
func (_m *TChanMetadataExposable) ReadConsumerGroup(ctx thrift.Context, getRequest *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	ret := _m.Called(ctx, getRequest)

	var r0 *shared.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadConsumerGroupRequest) *shared.ConsumerGroupDescription); ok {
		r0 = rf(ctx, getRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadConsumerGroupRequest) error); ok {
		r1 = rf(ctx, getRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroupByUUID provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadConsumerGroupByUUID(ctx thrift.Context, request *shared.ReadConsumerGroupRequest) (*shared.ConsumerGroupDescription, error) {
	ret := _m.Called(ctx, request)

	var r0 *shared.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadConsumerGroupRequest) *shared.ConsumerGroupDescription); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadConsumerGroupRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroupExtent provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadConsumerGroupExtent(ctx thrift.Context, request *metadata.ReadConsumerGroupExtentRequest) (*metadata.ReadConsumerGroupExtentResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadConsumerGroupExtentResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadConsumerGroupExtentRequest) *metadata.ReadConsumerGroupExtentResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadConsumerGroupExtentResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadConsumerGroupExtentRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroupExtents provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadConsumerGroupExtents(ctx thrift.Context, request *metadata.ReadConsumerGroupExtentsRequest) (*metadata.ReadConsumerGroupExtentsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadConsumerGroupExtentsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadConsumerGroupExtentsRequest) *metadata.ReadConsumerGroupExtentsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadConsumerGroupExtentsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadConsumerGroupExtentsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroupExtentsByExtUUID provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadConsumerGroupExtentsByExtUUID(ctx thrift.Context, request *metadata.ReadConsumerGroupExtentsByExtUUIDRequest) (*metadata.ReadConsumerGroupExtentsByExtUUIDResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadConsumerGroupExtentsByExtUUIDResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadConsumerGroupExtentsByExtUUIDRequest) *metadata.ReadConsumerGroupExtentsByExtUUIDResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadConsumerGroupExtentsByExtUUIDResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadConsumerGroupExtentsByExtUUIDRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadDestination provides a mock function with given fields: ctx, getRequest
func (_m *TChanMetadataExposable) ReadDestination(ctx thrift.Context, getRequest *shared.ReadDestinationRequest) (*shared.DestinationDescription, error) {
	ret := _m.Called(ctx, getRequest)

	var r0 *shared.DestinationDescription
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ReadDestinationRequest) *shared.DestinationDescription); ok {
		r0 = rf(ctx, getRequest)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ReadDestinationRequest) error); ok {
		r1 = rf(ctx, getRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadExtentStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadExtentStats(ctx thrift.Context, request *metadata.ReadExtentStatsRequest) (*metadata.ReadExtentStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadExtentStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadExtentStatsRequest) *metadata.ReadExtentStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadExtentStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadExtentStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadServiceConfig provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadServiceConfig(ctx thrift.Context, request *metadata.ReadServiceConfigRequest) (*metadata.ReadServiceConfigResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadServiceConfigResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadServiceConfigRequest) *metadata.ReadServiceConfigResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadServiceConfigResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadServiceConfigRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UUIDToHostAddr provides a mock function with given fields: ctx, hostUUID
func (_m *TChanMetadataExposable) UUIDToHostAddr(ctx thrift.Context, hostUUID string) (string, error) {
	ret := _m.Called(ctx, hostUUID)

	var r0 string
	if rf, ok := ret.Get(0).(func(thrift.Context, string) string); ok {
		r0 = rf(ctx, hostUUID)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, string) error); ok {
		r1 = rf(ctx, hostUUID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateServiceConfig provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) UpdateServiceConfig(ctx thrift.Context, request *metadata.UpdateServiceConfigRequest) error {
	ret := _m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.UpdateServiceConfigRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
