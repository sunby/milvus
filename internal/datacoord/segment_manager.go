// Copyright (C) 2019-2020 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

var errRemainInSufficient = func(requestRows int64) error {
	return fmt.Errorf("segment remaining is insufficient for %d", requestRows)
}

// Manager manage segment related operations.
type Manager interface {
	// AllocSegment allocate rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) (UniqueID, int64, Timestamp, error)
	// DropSegment drop the segment from allocator.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments sealed all segmetns of collection with collectionID and return sealed segments
	SealAllSegments(ctx context.Context, collectionID UniqueID) ([]UniqueID, error)
	// GetFlushableSegments return flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
	// ExpireAllocations notify segment status to expire old allocations
	ExpireAllocations(channel string, ts Timestamp) error
}

// allcation entry for segment Allocation record
type Allocation struct {
	numOfRows  int64
	expireTime Timestamp
}

// SegmentManager handles segment related logic
type SegmentManager struct {
	meta                *meta
	mu                  sync.RWMutex
	allocator           allocator
	segments            []UniqueID
	estimatePolicy      calUpperLimitPolicy
	allocPolicy         allocatePolicy
	segmentSealPolicies []segmentSealPolicy
	channelSealPolicies []channelSealPolicy
	flushPolicy         flushPolicy
	allocPool           sync.Pool
}

// allocOption allction option applies to `SegmentManager`
type allocOption interface {
	apply(manager *SegmentManager)
}

// allocFunc function shortcut for allocOption
type allocFunc func(manager *SegmentManager)

// implement allocOption
func (f allocFunc) apply(manager *SegmentManager) {
	f(manager)
}

// get allocOption with estimatePolicy
func withCalUpperLimitPolicy(policy calUpperLimitPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.estimatePolicy = policy })
}

// get allocOption with allocPolicy
func withAllocPolicy(policy allocatePolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.allocPolicy = policy })
}

// get allocOption with segmentSealPolicies
func withSegmentSealPolices(policies ...segmentSealPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) {
		// do override instead of append, to override default options
		manager.segmentSealPolicies = policies
	})
}

// get allocOption with channelSealPolicies
func withChannelSealPolices(policies ...channelSealPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) {
		// do override instead of append, to override default options
		manager.channelSealPolicies = policies
	})
}

// get allocOption with flushPolicy
func withFlushPolicy(policy flushPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.flushPolicy = policy })
}

func defaultCalUpperLimitPolicy() calUpperLimitPolicy {
	return calBySchemaPolicy
}

func defaultAlocatePolicy() allocatePolicy {
	return allocatePolicyV1
}

func defaultSealPolicy() sealPolicy {
	return sealPolicyV1
}

func defaultSegmentSealPolicy() segmentSealPolicy {
	return getSegmentCapacityPolicy(Params.SegmentSealProportion)
}

func defaultFlushPolicy() flushPolicy {
	return flushPolicyV1
}

// newSegmentManager should be the only way to retrieve SegmentManager
func newSegmentManager(meta *meta, allocator allocator, opts ...allocOption) *SegmentManager {
	manager := &SegmentManager{
		meta:                meta,
		allocator:           allocator,
		segments:            make([]UniqueID, 0),
		estimatePolicy:      defaultCalUpperLimitPolicy(),
		allocPolicy:         defaultAlocatePolicy(),
		segmentSealPolicies: []segmentSealPolicy{defaultSegmentSealPolicy()}, // default only segment size policy
		channelSealPolicies: []channelSealPolicy{},                           // no default channel seal policy
		flushPolicy:         defaultFlushPolicy(),
		allocPool: sync.Pool{
			New: func() interface{} {
				return &Allocation{}
			},
		},
	}
	for _, opt := range opts {
		opt.apply(manager)
	}
	manager.loadSegmentsFromMeta()
	return manager
}

// loadSegmentsFromMeta generate corresponding segment status for each segment from meta
func (s *SegmentManager) loadSegmentsFromMeta() {
	segments := s.meta.GetUnFlushedSegments()
	segmentsID := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		segmentsID = append(segmentsID, segment.GetID())
	}
	s.segments = segmentsID
}

// getAllocation unified way to retrieve allocation struct
func (s *SegmentManager) getAllocation(numOfRows int64) *Allocation {
	v := s.allocPool.Get()
	if v == nil {
		return &Allocation{
			numOfRows: numOfRows,
		}
	}
	a, ok := v.(*Allocation)
	if !ok {
		a = &Allocation{}
	}
	a.numOfRows = numOfRows
	return a
}

// putAllocation put allocation for recycling
func (s *SegmentManager) putAllocation(a *Allocation) {
	s.allocPool.Put(a)
}

// AllocSegment allocate segment per request collcation, partication, channel and rows
func (s *SegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64) (segID UniqueID, retCount int64, expireTime Timestamp, err error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()

	var segment *SegmentInfo
	var allocation *Allocation
	for _, segmentID := range s.segments {
		segment = s.meta.GetSegment(segmentID)
		if segment == nil {
			log.Warn("Failed to get seginfo from meta", zap.Int64("id", segmentID), zap.Error(err))
			continue
		}
		if segment.State == commonpb.SegmentState_Sealed || segment.CollectionID != collectionID ||
			segment.PartitionID != partitionID || segment.InsertChannel != channelName {
			continue
		}
		allocation, err = s.alloc(segment, requestRows)
		if err != nil {
			return
		}
		if allocation != nil {
			break
		}
	}

	if allocation == nil {
		segment, err = s.openNewSegment(ctx, collectionID, partitionID, channelName)
		if err != nil {
			return
		}
		segment = s.meta.GetSegment(segment.GetID())
		if segment == nil {
			log.Warn("Failed to get seg into from meta", zap.Int64("id", segment.GetID()), zap.Error(err))
			return
		}
		allocation, err = s.alloc(segment, requestRows)
		if err != nil {
			return
		}
		if allocation == nil {
			err = errRemainInSufficient(requestRows)
			return
		}
	}

	segID = segment.GetID()
	retCount = allocation.numOfRows
	expireTime = allocation.expireTime
	return
}

func (s *SegmentManager) alloc(segment *SegmentInfo, numOfRows int64) (*Allocation, error) {
	var allocSize int64
	for _, allocItem := range segment.allocations {
		allocSize += allocItem.numOfRows
	}

	if !s.allocPolicy(segment, numOfRows) {
		return nil, nil
	}

	alloc := s.getAllocation(numOfRows)
	expireTs, err := s.genExpireTs()
	if err != nil {
		return nil, err
	}
	alloc.expireTime = expireTs

	//safe here since info is a clone, used to pass expireTs out
	s.meta.AddAllocation(segment.GetID(), alloc)
	return alloc, nil
}

func (s *SegmentManager) genExpireTs() (Timestamp, error) {
	ts, err := s.allocator.allocTimestamp()
	if err != nil {
		return 0, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.SegAssignmentExpiration) * time.Millisecond)
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	return expireTs, nil
}

func (s *SegmentManager) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string) (*SegmentInfo, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	id, err := s.allocator.allocID()
	if err != nil {
		return nil, err
	}
	maxNumOfRows, err := s.estimateMaxNumOfRows(collectionID)
	if err != nil {
		return nil, err
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumOfRows:      0,
		State:          commonpb.SegmentState_Growing,
		MaxRowNum:      int64(maxNumOfRows),
		LastExpireTime: 0,
		StartPosition: &internalpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   0,
		},
	}
	segment := NewSegmentInfo(segmentInfo)
	if err := s.meta.AddSegment(segment); err != nil {
		return nil, err
	}
	s.segments = append(s.segments, id)
	log.Debug("datacoord: estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", maxNumOfRows),
		zap.String("Channel", segmentInfo.InsertChannel))

	return segment, nil
}

func (s *SegmentManager) estimateMaxNumOfRows(collectionID UniqueID) (int, error) {
	collMeta := s.meta.GetCollection(collectionID)
	if collMeta == nil {
		return -1, fmt.Errorf("Failed to get collection %d", collectionID)
	}
	return s.estimatePolicy(collMeta.Schema)
}

func (s *SegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, id := range s.segments {
		if id == segmentID {
			s.segments = append(s.segments[:i], s.segments[i+1:]...)
			break
		}
	}
	segment := s.meta.GetSegment(segmentID)
	if segment == nil {
		log.Warn("failed to get segment", zap.Int64("id", segmentID))
	}
	s.meta.SetAllocations(segmentID, []*Allocation{})
	for _, allocation := range segment.allocations {
		s.putAllocation(allocation)
	}
}

func (s *SegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := make([]UniqueID, 0)
	for _, id := range s.segments {
		info := s.meta.GetSegment(id)
		if info == nil {
			log.Warn("Failed to get seg info from meta", zap.Int64("id", id))
			continue
		}
		if info.CollectionID != collectionID {
			continue
		}
		if info.State == commonpb.SegmentState_Sealed {
			ret = append(ret, id)
			continue
		}
		if err := s.meta.SetState(id, commonpb.SegmentState_Sealed); err != nil {
			return nil, err
		}
		ret = append(ret, id)
	}
	return ret, nil
}

func (s *SegmentManager) GetFlushableSegments(ctx context.Context, channel string,
	t Timestamp) ([]UniqueID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	if err := s.tryToSealSegment(t); err != nil {
		return nil, err
	}

	ret := make([]UniqueID, 0, len(s.segments))
	for _, id := range s.segments {
		info := s.meta.GetSegment(id)
		if info == nil {
			continue
		}
		if s.flushPolicy(info, t) {
			ret = append(ret, id)
		}
	}

	return ret, nil
}

// ExpireAllocations notify segment status to expire old allocations
func (s *SegmentManager) ExpireAllocations(channel string, ts Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range s.segments {
		segment := s.meta.GetSegment(id)
		if segment == nil {
			continue
		}
		for i := 0; i < len(segment.allocations); i++ {
			if segment.allocations[i].expireTime <= ts {
				a := segment.allocations[i]
				segment.allocations = append(segment.allocations[:i], segment.allocations[i+1:]...)
				s.putAllocation(a)
			}
		}
		s.meta.SetAllocations(segment.GetID(), segment.allocations)
	}
	return nil
}

// tryToSealSegment applies segment & channel seal policies
func (s *SegmentManager) tryToSealSegment(ts Timestamp) error {
	channelInfo := make(map[string][]*SegmentInfo)
	for _, id := range s.segments {
		info := s.meta.GetSegment(id)
		if info == nil {
			log.Warn("Failed to get seg info from meta", zap.Int64("id", id))
			continue
		}
		channelInfo[info.InsertChannel] = append(channelInfo[info.InsertChannel], info)
		if info.State == commonpb.SegmentState_Sealed {
			continue
		}
		// change shouldSeal to segment seal policy logic
		for _, policy := range s.segmentSealPolicies {
			if policy(info, ts) {
				if err := s.meta.SetState(id, commonpb.SegmentState_Sealed); err != nil {
					return err
				}
				break
			}
		}
	}
	for channel, segmentInfos := range channelInfo {
		for _, policy := range s.channelSealPolicies {
			vs := policy(channel, segmentInfos, ts)
			for _, info := range vs {
				if info.State == commonpb.SegmentState_Sealed {
					continue
				}
				if err := s.meta.SetState(info.GetID(), commonpb.SegmentState_Sealed); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// only for test
func (s *SegmentManager) SealSegment(ctx context.Context, segmentID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.meta.SetState(segmentID, commonpb.SegmentState_Sealed); err != nil {
		return err
	}
	return nil
}
