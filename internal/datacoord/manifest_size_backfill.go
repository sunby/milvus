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
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// manifestSizeBackfillMaxConsecutiveFailures stops a run that keeps failing,
// so the Talon prewarm waiting for it starts; the next run retries.
const manifestSizeBackfillMaxConsecutiveFailures = 256

// manifestSize is the object size of the manifest revision a segment points
// to.
type manifestSize struct {
	segmentID    int64
	manifestPath string
	size         int64
}

// manifestSizeBackfill records, once, the object size in the manifest paths of
// live segments that record none: the paths committed before revisions
// recorded their size. Readers then open those manifests without asking
// storage for the size, and the Talon prewarm loads them without a stat.
type manifestSizeBackfill struct {
	meta        *meta
	size        func(ctx context.Context, key string) (int64, error)
	concurrency int
}

func newManifestSizeBackfill(p *paramtable.ComponentParam, m *meta) (*manifestSizeBackfill, error) {
	if !p.DataCoordCfg.ManifestSizeBackfillEnabled.GetAsBool() || p.CommonCfg.StorageType.GetValue() == "local" {
		return nil, nil
	}
	concurrency := p.DataCoordCfg.ManifestSizeBackfillConcurrency.GetAsInt()
	if concurrency < 1 || concurrency > 256 {
		return nil, merr.WrapErrServiceInternalMsg("dataCoord.manifestSizeBackfill.concurrency must be in [1, 256]")
	}
	return &manifestSizeBackfill{meta: m, size: m.chunkManager.Size, concurrency: concurrency}, nil
}

// recordsNoManifestSize reports whether segment is a live segment pointing to a
// published manifest revision whose path records no size.
func recordsNoManifestSize(segment *SegmentInfo) bool {
	if !hasLiveManifest(segment) {
		return false
	}
	manifest, err := packed.ParseManifestPath(segment.GetManifestPath())
	return err == nil && manifest.ManifestVersion > packed.ManifestEarliest && manifest.Size <= 0
}

// run sizes the manifests of the segments that record no size when it starts,
// concurrently, and records the sizes in batches. A manifest it fails to size
// keeps its path; a later run retries it.
func (b *manifestSizeBackfill) run(ctx context.Context) {
	start := time.Now()
	var segmentIDs []int64
	b.meta.segments.RangeReused(func(segment *SegmentInfo) bool {
		if recordsNoManifestSize(segment) {
			segmentIDs = append(segmentIDs, segment.GetID())
		}
		return ctx.Err() == nil
	})
	batchSize := max(1, Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt())
	mlog.Info(ctx, "manifest size backfill started", mlog.Int("segments", len(segmentIDs)), mlog.Int("concurrency", b.concurrency))

	var failures, consecutive atomic.Int64
	fail := func(segments int) {
		failures.Add(int64(segments))
		consecutive.Add(int64(segments))
	}
	sized := make(chan manifestSize, batchSize+b.concurrency)
	go func() {
		defer close(sized)
		var group errgroup.Group
		group.SetLimit(b.concurrency)
		for _, segmentID := range segmentIDs {
			if ctx.Err() != nil || consecutive.Load() >= manifestSizeBackfillMaxConsecutiveFailures {
				break
			}
			group.Go(func() error {
				// Size the revision the segment points to now; it may have
				// moved on since the scan.
				segment := b.meta.segments.GetSegment(segmentID)
				if !recordsNoManifestSize(segment) {
					return nil
				}
				key, err := packed.ManifestFilePath(segment.GetManifestPath())
				var size int64
				if err == nil {
					size, err = b.size(ctx, key)
				}
				if err != nil {
					fail(1)
					mlog.RatedWarn(ctx, 1, "manifest size backfill stat failed", mlog.FieldSegmentID(segmentID), mlog.Err(err))
					return nil
				}
				consecutive.Store(0)
				sized <- manifestSize{segmentID: segmentID, manifestPath: segment.GetManifestPath(), size: size}
				return nil
			})
		}
		_ = group.Wait()
	}()

	recorded := 0
	batch := make([]manifestSize, 0, batchSize)
	flush := func() {
		if len(batch) > 0 && ctx.Err() == nil {
			n, err := b.meta.RecordManifestSizes(ctx, batch)
			if err != nil {
				fail(len(batch))
				mlog.RatedWarn(ctx, 1, "manifest size backfill write failed", mlog.Int("count", len(batch)), mlog.Err(err))
			}
			recorded += n
			mlog.RatedInfo(ctx, rate.Every(time.Minute), "manifest size backfill progress", mlog.Int("segments", len(segmentIDs)),
				mlog.Int("recorded", recorded), mlog.Int64("failures", failures.Load()))
		}
		batch = batch[:0]
	}
	for size := range sized {
		batch = append(batch, size)
		if len(batch) == batchSize {
			flush()
		}
	}
	flush()
	mlog.Info(ctx, "manifest size backfill finished", mlog.Int("segments", len(segmentIDs)), mlog.Int("recorded", recorded),
		mlog.Int64("failures", failures.Load()), mlog.Duration("duration", time.Since(start)), mlog.Bool("canceled", ctx.Err() != nil),
		mlog.Bool("abandoned", consecutive.Load() >= manifestSizeBackfillMaxConsecutiveFailures))
}
