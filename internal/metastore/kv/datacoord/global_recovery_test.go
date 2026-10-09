// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestGlobalSegmentRecoveryMatchesCollectionReads(t *testing.T) {
	ctx := context.Background()
	values := make(map[string][]byte)
	for _, collection := range []int64{10, 20} {
		segment := proto.Clone(segment1).(*datapb.SegmentInfo)
		segment.CollectionID = collection
		segment.ID = collection + 1
		value, err := proto.Marshal(segment)
		require.NoError(t, err)
		values[buildSegmentPath(collection, segment.PartitionID, segment.ID)] = value
	}
	// Sibling metadata must not be decoded as segment records by the global scan.
	values[SegmentPrefix+"-unrelated/1"] = []byte("invalid protobuf")
	kv := mocks.NewMetaKv(t)
	var mu sync.Mutex
	prefixes := make(map[string]int)
	kv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, prefix string, _ int, visit func([]byte, []byte) error) error {
		mu.Lock()
		prefixes[prefix]++
		mu.Unlock()
		for key, value := range values {
			if strings.HasPrefix(key, prefix) {
				if err := visit([]byte(key), value); err != nil {
					return err
				}
			}
		}
		return nil
	})
	catalog := NewCatalog(kv, rootPath, "")
	global, err := catalog.ListAllSegments(ctx)
	require.NoError(t, err)
	require.Len(t, global, 2)
	require.Equal(t, map[string]int{
		SegmentPrefix + "/": 1, SegmentBinlogPathPrefix + "/": 1, SegmentDeltalogPathPrefix + "/": 1,
		SegmentStatslogPathPrefix + "/": 1, SegmentBM25logPathPrefix + "/": 1,
	}, prefixes)
	for _, collection := range []int64{10, 20} {
		local, err := catalog.ListSegments(ctx, collection)
		require.NoError(t, err)
		require.Len(t, local, 1)
		var matched *datapb.SegmentInfo
		for _, segment := range global {
			if segment.CollectionID == collection {
				matched = segment
			}
		}
		require.True(t, proto.Equal(local[0], matched))
	}
}

func TestGlobalRecoveryPropagatesScanFailures(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(merr.ErrServiceUnavailable)
	catalog := NewCatalog(kv, rootPath, "")
	segments, err := catalog.ListAllSegments(context.Background())
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, segments)
	indexes, err := catalog.ListAllSegmentIndexes(context.Background())
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, indexes)
}

func TestGlobalIndexRecoveryMatchesCollectionReads(t *testing.T) {
	values := make(map[string][]byte)
	for _, collection := range []int64{10, 20} {
		index := &model.SegmentIndex{CollectionID: collection, PartitionID: 1, SegmentID: collection + 1, BuildID: collection + 2, IndexID: collection + 3}
		value, err := proto.Marshal(model.MarshalSegmentIndexModel(index))
		require.NoError(t, err)
		values[BuildSegmentIndexKey(collection, 1, index.SegmentID, index.BuildID)] = value
	}
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, prefix string, _ int, visit func([]byte, []byte) error) error {
		for key, value := range values {
			if strings.HasPrefix(key, prefix) {
				if err := visit([]byte(key), value); err != nil {
					return err
				}
			}
		}
		return nil
	})
	catalog := NewCatalog(kv, rootPath, "")
	global, err := catalog.ListAllSegmentIndexes(context.Background())
	require.NoError(t, err)
	require.Len(t, global, 2)
	for _, collection := range []int64{10, 20} {
		local, err := catalog.ListSegmentIndexes(context.Background(), collection)
		require.NoError(t, err)
		require.Len(t, local, 1)
		require.Contains(t, global, local[0])
	}
}
