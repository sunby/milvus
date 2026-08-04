// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeBatchRecoveryManager struct {
	DataViewManager
	collectionIDs []int64
}

func (m *fakeBatchRecoveryManager) RecoverCollections(
	ctx context.Context,
	collectionIDs []int64,
	observe func(index int, collectionID int64, duration time.Duration, err error),
) error {
	m.collectionIDs = append([]int64(nil), collectionIDs...)
	for index, collectionID := range collectionIDs {
		observe(index, collectionID, 0, nil)
	}
	return nil
}

type fakePointRecoveryManager struct {
	DataViewManager
	collectionIDs []int64
}

func (m *fakePointRecoveryManager) RepairCollection(ctx context.Context, collectionID int64) error {
	m.collectionIDs = append(m.collectionIDs, collectionID)
	return nil
}

func TestRecoverDataViewCollectionsUsesOptionalBatchManager(t *testing.T) {
	manager := &fakeBatchRecoveryManager{}
	observed := 0

	err := recoverDataViewCollections(context.Background(), manager, []int64{1, 2}, func(index int, collectionID int64, duration time.Duration, err error) {
		require.NoError(t, err)
		observed++
	})

	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, manager.collectionIDs)
	require.Equal(t, 2, observed)
}

func TestRecoverDataViewCollectionsFallsBackToPointManager(t *testing.T) {
	manager := &fakePointRecoveryManager{}

	err := recoverDataViewCollections(context.Background(), manager, []int64{1, 2}, nil)

	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, manager.collectionIDs)
}
