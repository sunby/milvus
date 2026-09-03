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
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// GcOption garbage collection options
type GcOption struct {
	cli              storage.ChunkManager // client
	enabled          bool                 // enable switch
	checkInterval    time.Duration        // each interval
	missingTolerance time.Duration        // key missing in meta tolerance time
	dropTolerance    time.Duration        // dropped segment related key tolerance time
	scanInterval     time.Duration        // interval for scan residue for interupted log wrttien

	broker           broker.Broker
	removeObjectPool *conc.Pool[struct{}]
	dataViewGC       DataViewGarbageCollector
}

type DataViewGarbageCollector interface {
	ListGarbageCollectionCandidates(ctx context.Context, collectionIDs []int64, retainLatest int) (map[int64][]*viewpb.DataVersion, error)
	GarbageCollectCandidates(ctx context.Context, collectionID int64, candidates []*viewpb.DataVersion) error
}

// garbageCollector handles garbage files in object storage
// which could be dropped collection remanent or data node failure traces
type garbageCollector struct {
	ctx    context.Context
	cancel context.CancelFunc

	option  GcOption
	meta    *meta
	handler Handler

	startOnce        sync.Once
	stopOnce         sync.Once
	wg               sync.WaitGroup
	cmdCh            chan gcCmd
	pauseUntil       *gcPauseRecords
	pausedCollection *typeutil.ConcurrentMap[int64, *gcPauseRecords]
	controlChannels  map[string]chan gcCmd

	systemMetricsListener *hardware.SystemMetricsListener
}

type gcCmd struct {
	cmdType      datapb.GcCommand
	duration     time.Duration
	collectionID int64
	ticket       string
	done         chan error
	ctx          context.Context
	timeout      <-chan struct{}
}

type gcPauseRecord struct {
	// id uniquely identifies this record within its gcPauseRecords. Tickets are
	// not unique -- the REST route in restful_mgr_routes.go issues every pause
	// with an empty ticket -- so rollback must delete by id, not by ticket.
	id         int64
	ticket     string
	pauseUntil time.Time
}

type gcPauseRecords struct {
	mut     sync.RWMutex
	maxLen  int
	nextID  int64
	records typeutil.Heap[gcPauseRecord]
}

func (gc *gcPauseRecords) PauseUntil() time.Time {
	// nil protection
	if gc == nil {
		return time.Time{}
	}
	gc.mut.RLock()
	defer gc.mut.RUnlock()
	// no pause records, return zero value
	if gc.records.Len() == 0 {
		return time.Time{}
	}

	return gc.records.Peek().pauseUntil
}

// Insert records a pause ticket and returns the id of the record it created, so
// that a failed pause can roll back exactly its own record via DeleteByID.
func (gc *gcPauseRecords) Insert(ticket string, pauseUntil time.Time) (int64, error) {
	gc.mut.Lock()
	defer gc.mut.Unlock()

	// heap small enough, short path
	if gc.records.Len() < gc.maxLen {
		return gc.pushLocked(ticket, pauseUntil), nil
	}

	records := make([]gcPauseRecord, 0, gc.records.Len())
	now := time.Now()
	for gc.records.Len() > 0 {
		record := gc.records.Pop()
		if record.pauseUntil.After(now) {
			records = append(records, record)
		}
	}
	gc.records = typeutil.NewObjectArrayBasedMaximumHeap(records, func(r gcPauseRecord) int64 {
		return r.pauseUntil.UnixNano()
	})

	if gc.records.Len() < gc.maxLen {
		return gc.pushLocked(ticket, pauseUntil), nil
	}

	// too many pause records, refresh heap
	return 0, merr.WrapErrTooManyRequests(64, "too many pause records")
}

// pushLocked appends a new record with a freshly allocated id. Caller must hold mut.
func (gc *gcPauseRecords) pushLocked(ticket string, pauseUntil time.Time) int64 {
	gc.nextID++
	gc.records.Push(gcPauseRecord{
		id:         gc.nextID,
		ticket:     ticket,
		pauseUntil: pauseUntil,
	})
	return gc.nextID
}

// Delete drops every record holding the given ticket. This is the user-facing
// resume semantic ("release my ticket"); use DeleteByID to undo a single record.
func (gc *gcPauseRecords) Delete(ticket string) {
	gc.deleteMatching(func(r gcPauseRecord) bool { return r.ticket == ticket })
}

// DeleteByID drops the single record with the given id, leaving records that
// merely share its ticket untouched.
func (gc *gcPauseRecords) DeleteByID(id int64) {
	gc.deleteMatching(func(r gcPauseRecord) bool { return r.id == id })
}

// deleteMatching rebuilds the heap without the matching records, dropping
// already-expired records along the way.
func (gc *gcPauseRecords) deleteMatching(match func(gcPauseRecord) bool) {
	gc.mut.Lock()
	defer gc.mut.Unlock()
	now := time.Now()
	records := make([]gcPauseRecord, 0, gc.records.Len())
	for gc.records.Len() > 0 {
		record := gc.records.Pop()
		if now.Before(record.pauseUntil) && !match(record) {
			records = append(records, record)
		}
	}
	gc.records = typeutil.NewObjectArrayBasedMaximumHeap(records, func(r gcPauseRecord) int64 {
		return r.pauseUntil.UnixNano()
	})
}

func (gc *gcPauseRecords) Len() int {
	gc.mut.RLock()
	defer gc.mut.RUnlock()
	return gc.records.Len()
}

func NewGCPauseRecords() *gcPauseRecords {
	return &gcPauseRecords{
		records: typeutil.NewObjectArrayBasedMaximumHeap[gcPauseRecord, int64]([]gcPauseRecord{}, func(r gcPauseRecord) int64 {
			return r.pauseUntil.UnixNano()
		}),
		maxLen: 64,
	}
}

// newSystemMetricsListener creates a system metrics listener for garbage collector.
// used to slow down the garbage collector when cpu usage is high.
func newSystemMetricsListener(opt *GcOption) *hardware.SystemMetricsListener {
	return &hardware.SystemMetricsListener{
		Cooldown:  15 * time.Second,
		Context:   false,
		Condition: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) bool { return true },
		Callback: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) {
			isSlowDown := listener.Context.(bool)
			if metrics.UsedRatio() > paramtable.Get().DataCoordCfg.GCSlowDownCPUUsageThreshold.GetAsFloat() {
				if !isSlowDown {
					mlog.Info(context.TODO(), "garbage collector slow down...", mlog.Float64("cpuUsage", metrics.UsedRatio()))
					opt.removeObjectPool.Resize(1)
					listener.Context = true
				}
				return
			}
			if isSlowDown {
				mlog.Info(context.TODO(), "garbage collector slow down finished", mlog.Float64("cpuUsage", metrics.UsedRatio()))
				opt.removeObjectPool.Resize(paramtable.Get().DataCoordCfg.GCRemoveConcurrent.GetAsInt())
				listener.Context = false
			}
		},
	}
}

// newGarbageCollector create garbage collector with meta and option
func newGarbageCollector(meta *meta, handler Handler, opt GcOption) *garbageCollector {
	mlog.Info(context.TODO(), "GC with option",
		mlog.Bool("enabled", opt.enabled),
		mlog.Duration("interval", opt.checkInterval),
		mlog.Duration("scanInterval", opt.scanInterval),
		mlog.Duration("missingTolerance", opt.missingTolerance),
		mlog.Duration("dropTolerance", opt.dropTolerance))
	opt.removeObjectPool = conc.NewPool[struct{}](Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	metaSignal := make(chan gcCmd)
	orphanSignal := make(chan gcCmd)
	lobSignal := make(chan gcCmd)
	controlChannels := map[string]chan gcCmd{
		"meta":   metaSignal,
		"orphan": orphanSignal,
		"lob":    lobSignal,
	}
	return &garbageCollector{
		ctx:                   ctx,
		cancel:                cancel,
		meta:                  meta,
		handler:               handler,
		option:                opt,
		cmdCh:                 make(chan gcCmd),
		systemMetricsListener: newSystemMetricsListener(&opt),
		pauseUntil:            NewGCPauseRecords(),
		pausedCollection:      typeutil.NewConcurrentMap[int64, *gcPauseRecords](),
		controlChannels:       controlChannels,
	}
}

// start a goroutine and perform gc check every `checkInterval`
func (gc *garbageCollector) start() {
	if gc.option.enabled {
		if gc.option.cli == nil {
			mlog.Warn(gc.ctx, "DataCoord gc enabled, but SSO client is not provided")
			return
		}
		gc.startOnce.Do(func() {
			gc.work(gc.ctx)
		})
	}
}

// GcStatus holds the current status of the garbage collector.
type GcStatus struct {
	IsPaused      bool
	TimeRemaining time.Duration
}

// GetStatus returns the current status of the garbage collector.
func (gc *garbageCollector) GetStatus() GcStatus {
	pauseUntil := gc.pauseUntil.PauseUntil()
	now := time.Now()

	if now.Before(pauseUntil) {
		return GcStatus{
			IsPaused:      true,
			TimeRemaining: pauseUntil.Sub(now),
		}
	}

	return GcStatus{
		IsPaused:      false,
		TimeRemaining: 0,
	}
}

func (gc *garbageCollector) Pause(ctx context.Context, collectionID int64, ticket string, pauseDuration time.Duration) error {
	if !gc.option.enabled {
		mlog.Info(ctx, "garbage collection not enabled")
		return nil
	}
	done := make(chan error, 1)
	select {
	case gc.cmdCh <- gcCmd{
		cmdType:      datapb.GcCommand_Pause,
		duration:     pauseDuration,
		collectionID: collectionID,
		ticket:       ticket,
		done:         done,
		ctx:          ctx,
		timeout:      ctx.Done(),
	}:
		return <-done
	case <-ctx.Done():
		return ctx.Err()
	case <-gc.ctx.Done():
		// cmdCh is unbuffered and startControlLoop has already returned, so the
		// send above can never be received; without this arm the caller parks
		// until its own ctx is canceled (forever for a Done()-less ctx).
		return merr.WrapErrServiceUnavailable("garbage collector is closing")
	}
}

func (gc *garbageCollector) Resume(ctx context.Context, collectionID int64, ticket string) error {
	if !gc.option.enabled {
		mlog.Warn(ctx, "garbage collection not enabled, cannot resume")
		return merr.WrapErrServiceUnavailable("garbage collection not enabled")
	}
	done := make(chan error)
	select {
	case gc.cmdCh <- gcCmd{
		cmdType:      datapb.GcCommand_Resume,
		done:         done,
		collectionID: collectionID,
		ticket:       ticket,
		timeout:      ctx.Done(),
	}:
		<-done
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-gc.ctx.Done():
		// see Pause: the control loop is gone, nothing will receive this send.
		return merr.WrapErrServiceUnavailable("garbage collector is closing")
	}
}

// work contains actual looping check logic
func (gc *garbageCollector) work(ctx context.Context) {
	// TODO: fast cancel for gc when closing.
	// Run gc tasks in parallel.
	gc.wg.Add(4)
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "meta", gc.option.checkInterval, func(ctx context.Context, signal <-chan gcCmd) {
			runGCStage(ctx, "meta", "recycleDataViews", func() { gc.recycleDataViews(ctx, signal) })
			runGCStage(ctx, "meta", "recycleDroppedSegments", func() { gc.recycleDroppedSegments(ctx, signal) })
			runGCStage(ctx, "meta", "recycleChannelCPMeta", func() { gc.recycleChannelCPMeta(ctx, signal) })
			runGCStage(ctx, "meta", "recycleUnusedIndexes", func() { gc.recycleUnusedIndexes(ctx, signal) })
			runGCStage(ctx, "meta", "recycleUnusedSegIndexes", func() { gc.recycleUnusedSegIndexes(ctx, signal) })
			runGCStage(ctx, "meta", "recycleUnusedAnalyzeFiles", func() { gc.recycleUnusedAnalyzeFiles(ctx, signal) })
			runGCStage(ctx, "meta", "recycleUnusedTextIndexFiles", func() { gc.recycleUnusedTextIndexFiles(ctx, signal) })
			runGCStage(ctx, "meta", "recycleUnusedJSONIndexFiles", func() { gc.recycleUnusedJSONIndexFiles(ctx, signal) })
			runGCStage(ctx, "meta", "recycleUnusedJSONStatsFiles", func() { gc.recycleUnusedJSONStatsFiles(ctx, signal) })
			runGCStage(ctx, "meta", "recycleSnapshots", func() { gc.recycleSnapshots(ctx, signal) })
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "orphan", gc.option.scanInterval, func(ctx context.Context, signal <-chan gcCmd) {
			// orphan file not controlled by collection level pause for now
			runGCStage(ctx, "orphan", "recycleUnusedBinlogFiles", func() { gc.recycleUnusedBinlogFiles(ctx) })
			runGCStage(ctx, "orphan", "recycleUnusedIndexFilesV0", func() { gc.recycleUnusedIndexFilesV0(ctx) })
			runGCStage(ctx, "orphan", "recycleUnusedIndexFilesV1", func() { gc.recycleUnusedIndexFilesV1(ctx) })
		})
	}()
	go func() {
		defer gc.wg.Done()
		// LOB (TEXT column) file GC runs on its own interval
		lobCheckInterval := Params.DataCoordCfg.GCLOBCheckInterval.GetAsDuration(time.Second)
		gc.runRecycleTaskWithPauser(ctx, "lob", lobCheckInterval, func(ctx context.Context, signal <-chan gcCmd) {
			runGCStage(ctx, "lob", "recycleUnusedLOBFiles", func() { gc.recycleUnusedLOBFiles(ctx) })
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.startControlLoop(ctx)
	}()
}

func runGCStage(ctx context.Context, gcType, stage string, task func()) {
	start := time.Now()
	defer func() {
		mlog.Info(ctx, "garbage collector stage done",
			mlog.String("gcType", gcType),
			mlog.String("gcStage", stage),
			mlog.Duration("timeCost", time.Since(start)),
			mlog.Bool("canceled", ctx.Err() != nil))
	}()
	task()
}

func (gc *garbageCollector) ackSignal(signal <-chan gcCmd) {
	select {
	case cmd := <-signal:
		if cmd.done != nil {
			close(cmd.done)
		}
	default:
	}
}

// startControlLoop start a control loop for garbageCollector.
func (gc *garbageCollector) startControlLoop(_ context.Context) {
	hardware.RegisterSystemMetricsListener(gc.systemMetricsListener)
	defer hardware.UnregisterSystemMetricsListener(gc.systemMetricsListener)

	for {
		select {
		case cmd := <-gc.cmdCh:
			switch cmd.cmdType {
			case datapb.GcCommand_Pause:
				err := gc.pause(cmd)
				cmd.done <- err
			case datapb.GcCommand_Resume:
				gc.resume(cmd)
			}
			close(cmd.done)
		case <-gc.ctx.Done():
			mlog.Warn(gc.ctx, "garbage collector control loop quit")
			return
		}
	}
}

func (gc *garbageCollector) pause(cmd gcCmd) error {
	log := mlog.With(
		mlog.Int64("collectionID", cmd.collectionID),
		mlog.String("ticket", cmd.ticket),
	)
	reqPauseUntil := time.Now().Add(cmd.duration)
	log = log.With(
		mlog.Time("pauseUntil", reqPauseUntil),
		mlog.Duration("duration", cmd.duration),
	)
	var err error
	var recordID int64
	if cmd.collectionID <= 0 { // legacy pause all
		recordID, err = gc.pauseUntil.Insert(cmd.ticket, reqPauseUntil)
		log.Info(gc.ctx, "global pause ticket recorded")
	} else {
		curr, has := gc.pausedCollection.Get(cmd.collectionID)
		if !has {
			curr = NewGCPauseRecords()
			gc.pausedCollection.Insert(cmd.collectionID, curr)
		}
		recordID, err = curr.Insert(cmd.ticket, reqPauseUntil)
		log.Info(gc.ctx, "collection new pause ticket recorded")
	}
	if err != nil {
		return err
	}
	signalCh := gc.controlChannels["meta"]
	// send signal to worker
	// make sure worker ack the pause command before returning
	signal := gcCmd{
		done:    make(chan error),
		timeout: cmd.timeout,
	}
	select {
	case signalCh <- signal:
		select {
		case <-signal.done:
		case <-cmd.timeout:
			gc.rollbackPause(cmd, recordID)
			return cmd.ctx.Err()
		}
	case <-cmd.timeout:
		// timeout, resume the pause
		gc.rollbackPause(cmd, recordID)
		return cmd.ctx.Err()
	case <-gc.ctx.Done():
		// The collector is closing. The meta worker may already have returned on
		// its own ctx.Done(), and signalCh is unbuffered, so the send above would
		// never be received: bound the wait by the collector's lifetime instead of
		// parking this control-loop goroutine and hanging gc.wg.Wait() in close().
		gc.rollbackPause(cmd, recordID)
		return merr.WrapErrServiceUnavailable("garbage collector is closing")
	}
	return nil
}

// rollbackPause undoes exactly the record this pause() call inserted. It must not
// go through resume(), whose delete is ticket-scoped: tickets are not unique --
// every pause issued by the REST route in restful_mgr_routes.go carries an empty
// ticket -- so a ticket-scoped delete would also drop a concurrent caller's
// still-valid pause record and resume GC while that caller believes it is paused.
func (gc *garbageCollector) rollbackPause(cmd gcCmd, recordID int64) {
	if cmd.collectionID <= 0 {
		gc.pauseUntil.DeleteByID(recordID)
	} else if curr, has := gc.pausedCollection.Get(cmd.collectionID); has {
		curr.DeleteByID(recordID)
		if curr.Len() == 0 || time.Now().After(curr.PauseUntil()) {
			gc.pausedCollection.Remove(cmd.collectionID)
		}
	}
	mlog.Info(gc.ctx, "pause rolled back",
		mlog.Int64("collectionID", cmd.collectionID),
		mlog.String("ticket", cmd.ticket))
}

func (gc *garbageCollector) resume(cmd gcCmd) {
	// reset to zero value
	var afterResume time.Time
	if cmd.collectionID <= 0 {
		gc.pauseUntil.Delete(cmd.ticket)
		afterResume = gc.pauseUntil.PauseUntil()
	} else {
		curr, has := gc.pausedCollection.Get(cmd.collectionID)
		if has {
			curr.Delete(cmd.ticket)
			afterResume = curr.PauseUntil()
			if curr.Len() == 0 || time.Now().After(afterResume) {
				gc.pausedCollection.Remove(cmd.collectionID)
			}
		}
	}
	stillPaused := time.Now().Before(afterResume)
	mlog.Info(gc.ctx, "garbage collection resumed", mlog.Bool("stillPaused", stillPaused))
}

// runRecycleTaskWithPauser is a helper function to create a task with pauser
func (gc *garbageCollector) runRecycleTaskWithPauser(ctx context.Context, name string, interval time.Duration, task func(ctx context.Context, signal <-chan gcCmd)) {
	logger := mlog.With(mlog.String("gcType", name)).With(mlog.Duration("interval", interval))
	timer := time.NewTicker(interval)
	defer timer.Stop()
	// get signal channel, ok if nil, means no control
	signal := gc.controlChannels[name]
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-signal:
			// notify signal received
			close(cmd.done)
		case <-timer.C:
			globalPauseUntil := gc.pauseUntil.PauseUntil()
			if time.Now().Before(globalPauseUntil) {
				logger.Info(ctx, "garbage collector paused", mlog.Time("until", globalPauseUntil))
				continue
			}
			logger.Info(ctx, "garbage collector recycle task start...")
			start := time.Now()
			task(ctx, signal)
			logger.Info(ctx, "garbage collector recycle task done", mlog.Duration("timeCost", time.Since(start)))
		}
	}
}

func (gc *garbageCollector) collectionGCPaused(collectionID int64) bool {
	collPauseUntil, has := gc.pausedCollection.Get(collectionID)
	return has && time.Now().Before(collPauseUntil.PauseUntil())
}

// close stop the garbage collector.
func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		gc.cancel()
		gc.wg.Wait()
		if gc.option.removeObjectPool != nil {
			gc.option.removeObjectPool.Release()
		}
	})
}

// recycleUnusedBinlogFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedBinlogFiles(ctx context.Context) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedBinlogFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedBinlogFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedBinlogFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	type scanTask struct {
		prefix            string
		checker           func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool
		segmentIDFromPath func(rootPath, filePath string) (int64, error)
		label             string
	}
	scanTasks := []scanTask{
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentInsertLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				return segment != nil
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.InsertFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentStatslogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn(ctx, "garbageCollector find dirty stats log", mlog.String("filePath", objectInfo.FilePath), mlog.Err(err))
					return false
				}
				return segment != nil && segment.IsStatsLogExists(logID)
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.StatFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentDeltaLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn(ctx, "garbageCollector find dirty dleta log", mlog.String("filePath", objectInfo.FilePath), mlog.Err(err))
					return false
				}
				return segment != nil && segment.IsDeltaLogExists(logID)
			},
			segmentIDFromPath: storage.ParseSegmentIDByBinlog,
			label:             metrics.DeleteFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.TextIndexPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				if segment == nil {
					return false
				}
				_, ok := getTextLogPaths(segment, gc.option.cli.RootPath())[objectInfo.FilePath]
				return ok
			},
			segmentIDFromPath: parseSegmentIDFromTextIndexPath,
			label:             common.TextIndexPath,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.JSONStatsPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				if segment == nil {
					return false
				}
				_, ok := getJSONKeyLogs(segment, gc)[objectInfo.FilePath]
				return ok
			},
			segmentIDFromPath: parseSegmentIDFromJSONStatsPath,
			label:             common.JSONStatsPath,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.JSONIndexPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				if segment == nil {
					return false
				}
				_, ok := getJSONKeyLogs(segment, gc)[objectInfo.FilePath]
				return ok
			},
			segmentIDFromPath: parseSegmentIDFromJSONIndexPath,
			label:             common.JSONIndexPath,
		},
	}

	for _, task := range scanTasks {
		gc.recycleUnusedBinLogWithChecker(ctx, task.prefix, task.label, task.segmentIDFromPath, task.checker)
	}
	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedBinLogWithChecker scans the prefix and checks the path with checker.
// GC the file if checker returns false.
func (gc *garbageCollector) recycleUnusedBinLogWithChecker(ctx context.Context, prefix string, label string, segmentIDFromPath func(rootPath, filePath string) (int64, error), checker func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool) {
	logger := mlog.With(mlog.String("prefix", prefix))
	logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles start", mlog.String("prefix", prefix))
	lastFilePath := ""
	total := 0
	valid := 0
	unexpectedFailure := atomic.NewInt32(0)
	removed := atomic.NewInt32(0)
	start := time.Now()

	// isSnapshotProtected checks if a segment should be skipped from GC due to snapshot references.
	// Returns true if the segment is protected (should NOT be deleted).
	//
	// Delegates to snapshotMeta.IsSegmentGCBlocked, which is O(1) and handles the
	// fail-closed layering (unloaded RefIndex → coarse collection-level block, else
	// point query on the pre-computed segmentReferencedByGC set). The per-call caching
	// that used to be needed here is no longer necessary because the lookups are now
	// direct set/map reads.
	snapshotMeta := gc.meta.GetSnapshotMeta()
	isSnapshotProtected := func(segmentID, collectionID int64) bool {
		if snapshotMeta == nil {
			return false
		}
		return snapshotMeta.IsSegmentGCBlocked(collectionID, segmentID)
	}

	futures := make([]*conc.Future[struct{}], 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(chunkInfo *storage.ChunkObjectInfo) bool {
		total++
		lastFilePath = chunkInfo.FilePath

		// Check file tolerance first to avoid unnecessary operation.
		if time.Since(chunkInfo.ModifyTime) <= gc.option.missingTolerance {
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles skip file since it is not expired", mlog.String("filePath", chunkInfo.FilePath), mlog.Time("modifyTime", chunkInfo.ModifyTime))
			return true
		}

		// Parse segmentID from file path.
		// TODO: Does all files in the same segment have the same segmentID?
		segmentID, err := segmentIDFromPath(gc.option.cli.RootPath(), chunkInfo.FilePath)
		if err != nil {
			// Try V3 path format: insert_log/{coll}/{part}/{seg}/...
			v3SegID, parseErr := parseV3SegmentID(gc.option.cli.RootPath(), chunkInfo.FilePath)
			if parseErr != nil {
				unexpectedFailure.Inc()
				logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles parse segment id error",
					mlog.String("filePath", chunkInfo.FilePath),
					mlog.Err(err))
				return true
			}
			if v3Seg := gc.meta.GetSegment(ctx, v3SegID); v3Seg != nil {
				if v3Seg.GetStorageVersion() == storage.StorageV3 {
					// registered V3 segment file — skip, live files are managed by
					// loon and dropped V3 segments are recycled with the whole
					// basePath by recycleDroppedSegments
					valid++
					return true
				}
				unexpectedFailure.Inc()
				logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles parse segment id error",
					mlog.String("filePath", chunkInfo.FilePath),
					mlog.Err(err))
				return true
			}
			// Orphan V3 file: its segment was never registered in meta, e.g.
			// output uploaded by a failed sort/mix compaction attempt (issue
			// #50962). Nothing manages it, so recycle it like V1/V2 orphans:
			// fall through to the shared checker/removal path below.
			segmentID = v3SegID
		}

		segment := gc.meta.GetSegment(ctx, segmentID)

		// Skip V3 segments — orphan files managed by loon
		if segment != nil && segment.GetStorageVersion() == storage.StorageV3 {
			valid++
			return true
		}

		if checker(chunkInfo, segment) {
			valid++
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles skip file since it is valid", mlog.String("filePath", chunkInfo.FilePath), mlog.Int64("segmentID", segmentID))
			return true
		}

		// Check if segment is referenced by any snapshot before deleting its binlog.
		collectionID := int64(-1)
		if segment != nil {
			collectionID = segment.GetCollectionID()
		}
		if isSnapshotProtected(segmentID, collectionID) {
			logger.Info(ctx, "skip GC binlog files since segment is protected by snapshot",
				mlog.Int64("segmentID", segmentID))
			valid++
			return true
		}

		// ignore error since it could be cleaned up next time
		file := chunkInfo.FilePath

		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			logger := logger.With(mlog.String("file", file))
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles remove file...")

			if err = gc.option.cli.Remove(ctx, file); err != nil {
				logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles remove file failed", mlog.Err(err))
				unexpectedFailure.Inc()
				return struct{}{}, err
			}
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles remove file success")
			removed.Inc()
			return struct{}{}, nil
		})
		futures = append(futures, future)
		return true
	})
	// Wait for all remove tasks done.
	if err := conc.BlockOnAll(futures...); err != nil {
		// error is logged, and can be ignored here.
		logger.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
	}

	cost := time.Since(start)
	logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles done",
		mlog.Int("total", total),
		mlog.Int("valid", valid),
		mlog.Int("unexpectedFailure", int(unexpectedFailure.Load())),
		mlog.Int("removed", int(removed.Load())),
		mlog.String("lastFilePath", lastFilePath),
		mlog.Duration("cost", cost),
		mlog.Err(err))

	metrics.GarbageCollectorFileScanDuration.
		WithLabelValues(paramtable.GetStringNodeID(), label).
		Observe(float64(cost.Milliseconds()))
}

func (gc *garbageCollector) recycleDataViews(ctx context.Context, signal <-chan gcCmd) {
	if gc.meta == nil || gc.option.dataViewGC == nil {
		return
	}
	start := time.Now()
	logger := mlog.With(mlog.String("gcName", "recycleDataViews"), mlog.Time("startAt", start))
	logger.Info(ctx, "start recycleDataViews")
	collections := gc.meta.GetCollections()
	totalCollections := len(collections)
	processedCollections := 0
	skippedCollections := 0
	failedCollections := 0
	candidateCollections := 0
	candidateVersions := 0
	defer func() {
		logger.Info(ctx, "recycleDataViews done",
			mlog.Int("totalCollections", totalCollections),
			mlog.Int("processedCollections", processedCollections),
			mlog.Int("skippedCollections", skippedCollections),
			mlog.Int("failedCollections", failedCollections),
			mlog.Int("candidateCollections", candidateCollections),
			mlog.Int("candidateVersions", candidateVersions),
			mlog.Duration("timeCost", time.Since(start)))
	}()
	progressEvery := max(totalCollections/10, 1)
	reportProgress := func() {
		if processedCollections < totalCollections && processedCollections%progressEvery == 0 {
			logger.Info(ctx, "recycleDataViews progress",
				mlog.Int("totalCollections", totalCollections),
				mlog.Int("processedCollections", processedCollections),
				mlog.Int("skippedCollections", skippedCollections),
				mlog.Int("failedCollections", failedCollections),
				mlog.Duration("timeCost", time.Since(start)))
		}
	}
	collectionIDs := make([]int64, 0, totalCollections)
	for _, collection := range collections {
		collectionIDs = append(collectionIDs, collection.ID)
	}
	candidateScanStart := time.Now()
	candidatesByCollection, err := gc.option.dataViewGC.ListGarbageCollectionCandidates(ctx, collectionIDs, 1)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		failedCollections = totalCollections
		logger.Warn(ctx, "DataView GC candidate scan failed",
			mlog.Int("totalCollections", totalCollections),
			mlog.Duration("timeCost", time.Since(candidateScanStart)),
			mlog.Err(err))
		return
	}
	candidateCollections = len(candidatesByCollection)
	for _, candidates := range candidatesByCollection {
		candidateVersions += len(candidates)
	}
	logger.Info(ctx, "DataView GC candidate scan done",
		mlog.Int("totalCollections", totalCollections),
		mlog.Int("candidateCollections", candidateCollections),
		mlog.Int("candidateVersions", candidateVersions),
		mlog.Duration("timeCost", time.Since(candidateScanStart)))

	for _, collection := range collections {
		if ctx.Err() != nil {
			return
		}
		gc.ackSignal(signal)

		collectionID := collection.ID
		if gc.collectionGCPaused(collectionID) {
			skippedCollections++
			processedCollections++
			reportProgress()
			logger.Info(ctx, "skip DataView GC since collection is paused", mlog.FieldCollectionID(collectionID))
			continue
		}

		candidates := candidatesByCollection[collectionID]
		if len(candidates) == 0 {
			processedCollections++
			reportProgress()
			continue
		}
		if err := gc.option.dataViewGC.GarbageCollectCandidates(ctx, collectionID, candidates); err != nil {
			failedCollections++
			logger.Warn(ctx, "DataView GC failed", mlog.FieldCollectionID(collectionID), mlog.Err(err))
		}
		processedCollections++
		reportProgress()
	}
}

func (gc *garbageCollector) checkDroppedSegmentGC(segment *SegmentInfo,
	childSegment *SegmentInfo,
	indexSet typeutil.UniqueSet,
	cpTimestamp Timestamp,
	channelExists bool,
) bool {
	log := mlog.With(mlog.FieldSegmentID(segment.ID))

	if !gc.isExpire(segment.GetDroppedAt()) {
		return false
	}
	isCompacted := childSegment != nil || segment.GetCompacted()
	if isCompacted {
		// For compact A, B -> C, don't GC A or B if C is not indexed,
		// guarantee replacing A, B with C won't downgrade performance
		// If the child is GC'ed first, then childSegment will be nil.
		if childSegment != nil && !indexSet.Contain(childSegment.GetID()) {
			log.RatedInfo(gc.ctx, rate.Limit(60), "skipping GC when compact target segment is not indexed",
				mlog.Int64("child segment ID", childSegment.GetID()))
			return false
		}
	}

	// Ignore segments from potentially dropped collection. Check if collection is to be dropped by checking if channel is dropped.
	// We do this because collection meta drop relies on all segment being GCed.
	if channelExists &&
		segmentEffectiveDmlTs(segment.SegmentInfo) > cpTimestamp {
		// segment gc shall only happen when channel cp is after segment dml cp.
		log.RatedInfo(gc.ctx, rate.Limit(60), "dropped segment dml position after channel cp, skip meta gc",
			mlog.Uint64("dmlPosTs", segmentEffectiveDmlTs(segment.SegmentInfo)),
			mlog.Uint64("channelCpTs", cpTimestamp),
		)
		return false
	}
	return true
}

type droppedSegmentGCChannelState struct {
	checkpoint Timestamp
	exists     bool
	loadErr    error
}

// recycleDroppedSegments scans all segments and remove those dropped segments from meta and oss.
func (gc *garbageCollector) recycleDroppedSegments(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleDroppedSegments"), mlog.Time("startAt", start))
	log.Info(ctx, "start clear dropped segments...")
	defer func() {
		log.Info(ctx, "clear dropped segments done", mlog.Duration("timeCost", time.Since(start)))
	}()

	metadataScanStart := time.Now()
	all := gc.meta.SelectSegments(ctx)
	drops := make(map[int64]*SegmentInfo, 0)
	compactTo := make(map[int64]*SegmentInfo)
	channels := typeutil.NewSet[string]()
	for _, segment := range all {
		if segment.GetState() == commonpb.SegmentState_Dropped {
			drops[segment.GetID()] = segment
			channels.Insert(segment.GetInsertChannel())
			// continue
			// A(indexed), B(indexed) -> C(no indexed), D(no indexed) -> E(no indexed), A, B can not be GC
		}
		for _, from := range segment.GetCompactionFrom() {
			compactTo[from] = segment
		}
	}
	log.Info(ctx, "recycleDroppedSegments metadata scan done",
		mlog.Int("totalSegments", len(all)),
		mlog.Int("droppedSegments", len(drops)),
		mlog.Int("compactionSources", len(compactTo)),
		mlog.Int("channels", channels.Len()),
		mlog.Duration("timeCost", time.Since(metadataScanStart)))

	protectionSetupStart := time.Now()
	droppedCompactTo := make(map[int64]*SegmentInfo)
	for id := range drops {
		if to, ok := compactTo[id]; ok {
			droppedCompactTo[to.GetID()] = to
		}
	}
	indexedSegments := FilterInIndexedSegments(ctx, gc.handler, gc.meta, false, lo.Values(droppedCompactTo)...)
	if ctx.Err() != nil {
		return
	}
	indexedSet := make(typeutil.UniqueSet)
	for _, segment := range indexedSegments {
		indexedSet.Insert(segment.GetID())
	}

	channelStates := make(map[string]droppedSegmentGCChannelState)
	for channel := range channels {
		pos := gc.meta.GetChannelCheckpoint(channel)
		channelStates[channel] = droppedSegmentGCChannelState{checkpoint: pos.GetTimestamp()}
	}

	// try to get loaded segments
	loadedSegments := typeutil.NewSet[int64]()
	segments, err := gc.handler.ListLoadedSegments(ctx)
	if err != nil {
		log.Warn(ctx, "failed to get loaded segments", mlog.Err(err))
		return
	}
	for _, segmentID := range segments {
		loadedSegments.Insert(segmentID)
	}
	log.Info(ctx, "recycleDroppedSegments protection setup done",
		mlog.Int("indexedCompactTargets", indexedSet.Len()),
		mlog.Int("loadedSegments", loadedSegments.Len()),
		mlog.Int("channels", len(channelStates)),
		mlog.Duration("timeCost", time.Since(protectionSetupStart)))

	log.Info(ctx, "start to GC segments", mlog.Int("drop_num", len(drops)))
	candidateStart := time.Now()
	processedDroppedSegments := 0
	channelStateBatchSize := Params.DataCoordCfg.GCDroppedSegmentChannelStateBatchSize.GetAsInt()
	defer func() {
		log.Info(ctx, "recycleDroppedSegments candidates done",
			mlog.Int("droppedSegments", len(drops)),
			mlog.Int("processedDroppedSegments", processedDroppedSegments),
			mlog.Int("channelStateBatchSize", channelStateBatchSize),
			mlog.Duration("timeCost", time.Since(candidateStart)))
	}()
	processedDroppedSegments = gc.recycleDroppedSegmentsInBatches(
		ctx,
		signal,
		drops,
		compactTo,
		indexedSet,
		channelStates,
		loadedSegments,
		channelStateBatchSize,
	)
}

func (gc *garbageCollector) isDroppedSegmentGCCandidate(
	ctx context.Context,
	segmentID int64,
	segment *SegmentInfo,
	compactTo map[int64]*SegmentInfo,
	indexedSet typeutil.UniqueSet,
	channelState droppedSegmentGCChannelState,
	loadedSegments typeutil.Set[int64],
) bool {
	if ctx.Err() != nil {
		return false
	}

	log := mlog.With(mlog.FieldSegmentID(segmentID))
	segInsertChannel := segment.GetInsertChannel()
	if channelState.loadErr != nil {
		// Fail closed. A transient metadata read error must not be interpreted as
		// an absent channel marker, especially because the state is shared by all
		// segments on this channel for the current GC pass.
		return false
	}
	if loadedSegments.Contain(segmentID) {
		log.RatedInfo(ctx, rate.Limit(1), "skip GC segment since it is loaded")
		return false
	}

	if gc.meta.dataViewManager != nil {
		referenced, err := gc.meta.dataViewManager.IsSegmentReferenced(ctx, segment.GetCollectionID(), segmentID)
		if err != nil {
			log.RatedWarn(ctx, rate.Limit(1), "skip GC segment since DataView reference check failed",
				mlog.FieldCollectionID(segment.GetCollectionID()),
				mlog.FieldPartitionID(segment.GetPartitionID()),
				mlog.FieldVChannel(segInsertChannel),
				mlog.Err(err))
			return false
		}
		if referenced {
			log.RatedInfo(ctx, rate.Limit(1), "skip GC segment since it is referenced by retained DataView",
				mlog.FieldCollectionID(segment.GetCollectionID()),
				mlog.FieldPartitionID(segment.GetPartitionID()),
				mlog.FieldVChannel(segInsertChannel))
			return false
		}
	}
	// Skip segments protected by snapshot references. IsSegmentGCBlocked is O(1)
	// and embeds the "RefIndex not loaded -> fail-closed" check, so we don't need
	// a separate loaded-state probe.
	if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil {
		if snapshotMeta.IsSegmentGCBlocked(segment.GetCollectionID(), segmentID) {
			log.RatedInfo(ctx, rate.Limit(1), "skip GC segment since it is protected by snapshot",
				mlog.FieldCollectionID(segment.GetCollectionID()),
				mlog.FieldPartitionID(segment.GetPartitionID()),
				mlog.FieldVChannel(segInsertChannel))
			return false
		}
	}

	if !gc.checkDroppedSegmentGC(
		segment,
		compactTo[segment.GetID()],
		indexedSet,
		channelState.checkpoint,
		channelState.exists,
	) {
		return false
	}
	return true
}

func (gc *garbageCollector) loadDroppedSegmentChannelStates(
	ctx context.Context,
	signal <-chan gcCmd,
	channelStates map[string]droppedSegmentGCChannelState,
	batchSize int,
) {
	if batchSize <= 0 {
		batchSize = 1
	}
	channels := make([]string, 0, batchSize)
	flush := func() bool {
		if len(channels) == 0 {
			gc.ackSignal(signal)
			return ctx.Err() == nil
		}

		gc.ackSignal(signal)
		existence, batchErr := gc.meta.catalog.LoadChannelExistence(ctx, channels)
		failedChannels := 0
		var missingResultErr error
		for _, channel := range channels {
			state := channelStates[channel]
			exists, ok := existence[channel]
			if ok {
				state.exists = exists
			} else {
				state.loadErr = batchErr
				if state.loadErr == nil {
					if missingResultErr == nil {
						missingResultErr = merr.WrapErrServiceInternalMsg(
							"channel existence batch omitted a requested channel",
						)
					}
					state.loadErr = missingResultErr
				}
				failedChannels++
			}
			channelStates[channel] = state
		}
		if failedChannels > 0 && ctx.Err() == nil {
			loadErr := batchErr
			if loadErr == nil {
				loadErr = missingResultErr
			}
			mlog.RatedWarn(ctx, rate.Limit(1), "skip dropped segment GC for channels whose batch state lookup failed",
				mlog.Int("channels", failedChannels),
				mlog.Err(loadErr))
		}
		channels = channels[:0]
		gc.ackSignal(signal)
		return ctx.Err() == nil
	}

	for channel := range channelStates {
		channels = append(channels, channel)
		if len(channels) == batchSize && !flush() {
			return
		}
	}
	flush()
}

type droppedSegmentGCBatchCandidate struct {
	segmentID   int64
	segment     *SegmentInfo
	segIndexes  []*model.SegmentIndex
	exactFiles  []string
	prefix      string
	fileDeleted bool
}

func (gc *garbageCollector) prepareDroppedSegmentGCBatchCandidate(
	ctx context.Context,
	segmentID int64,
	segment *SegmentInfo,
) (*droppedSegmentGCBatchCandidate, error) {
	segIndexes, indexFiles, indexSnapshotBlocked := gc.getDroppedSegmentIndexFiles(segmentID)
	if indexSnapshotBlocked {
		mlog.RatedInfo(ctx, rate.Limit(1), "skip GC segment since segment index is protected by snapshot",
			mlog.FieldSegmentID(segmentID),
			mlog.Int("segmentIndexes", len(segIndexes)))
		return nil, nil
	}

	cloned := segment.Clone()
	filePlan, err := gc.buildDroppedSegmentFilePlan(cloned, indexFiles)
	if err != nil {
		return nil, err
	}
	exactFiles := make([]string, 0, len(filePlan.exactFiles))
	for filePath := range filePlan.exactFiles {
		exactFiles = append(exactFiles, filePath)
	}
	return &droppedSegmentGCBatchCandidate{
		segmentID:  segmentID,
		segment:    cloned,
		segIndexes: segIndexes,
		exactFiles: exactFiles,
		prefix:     filePlan.prefix,
	}, nil
}

func collectBatchRemoveOutcomes(
	expectedPaths []string,
	results []storage.RemoveResult,
) (map[string]removeOutcome, error) {
	expected := make(map[string]struct{}, len(expectedPaths))
	for _, filePath := range expectedPaths {
		expected[filePath] = struct{}{}
	}
	outcomes := make(map[string]removeOutcome, len(expected))
	var batchErr error
	for _, result := range results {
		if result.Path == "" {
			if result.Err != nil {
				batchErr = merr.Combine(batchErr, result.Err)
			}
			continue
		}
		if _, ok := expected[result.Path]; !ok {
			if result.Err != nil {
				batchErr = merr.Combine(batchErr, result.Err)
			}
			continue
		}

		outcome := outcomes[result.Path]
		outcome.seen = true
		if result.Err != nil && !errors.Is(result.Err, merr.ErrIoKeyNotFound) {
			outcome.err = merr.Combine(outcome.err, result.Err)
		}
		outcomes[result.Path] = outcome
	}
	return outcomes, batchErr
}

// removeObjectFilesWithResult keeps the batch GC pipeline independent from
// optional storage capabilities. Backends with per-path batch results use
// their native implementation; every other ChunkManager is adapted with the
// existing bounded object-removal pool.
func (gc *garbageCollector) removeObjectFilesWithResult(
	ctx context.Context,
	filePaths []string,
) []storage.RemoveResult {
	if len(filePaths) == 0 {
		return nil
	}
	if batchRemover, ok := gc.option.cli.(storage.BatchRemoveChunkManager); ok {
		return batchRemover.MultiRemoveWithResult(ctx, filePaths)
	}

	results := make([]storage.RemoveResult, len(filePaths))
	futures := make([]*conc.Future[struct{}], 0, len(filePaths))
	for i, filePath := range filePaths {
		i, filePath := i, filePath
		futures = append(futures, gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			err := gc.option.cli.Remove(ctx, filePath)
			if errors.Is(err, merr.ErrIoKeyNotFound) {
				err = nil
			}
			results[i] = storage.RemoveResult{Path: filePath, Err: err}
			return struct{}{}, nil
		}))
	}
	_ = conc.BlockOnAll(futures...)
	return results
}

func batchRemovePathError(filePath string, outcomes map[string]removeOutcome, batchErr error) error {
	if batchErr != nil {
		return batchErr
	}
	outcome := outcomes[filePath]
	if !outcome.seen {
		return merr.WrapErrIoFailedMsg("batch delete returned no result for %s", filePath)
	}
	return outcome.err
}

func collectBatchRemovePrefixOutcomes(
	expectedPrefixes []string,
	results []storage.RemovePrefixResult,
) (map[string]removeOutcome, error) {
	expected := make(map[string]struct{}, len(expectedPrefixes))
	for _, prefix := range expectedPrefixes {
		expected[prefix] = struct{}{}
	}
	outcomes := make(map[string]removeOutcome, len(expected))
	var batchErr error
	for _, result := range results {
		if result.Prefix == "" {
			if result.Err != nil {
				batchErr = merr.Combine(batchErr, result.Err)
			}
			continue
		}
		if _, ok := expected[result.Prefix]; !ok {
			if result.Err != nil {
				batchErr = merr.Combine(batchErr, result.Err)
			}
			continue
		}

		outcome := outcomes[result.Prefix]
		outcome.seen = true
		if result.Err != nil && !errors.Is(result.Err, merr.ErrIoKeyNotFound) {
			outcome.err = merr.Combine(outcome.err, result.Err)
		}
		outcomes[result.Prefix] = outcome
	}
	return outcomes, batchErr
}

func (gc *garbageCollector) recycleDroppedSegmentBatch(
	ctx context.Context,
	batch []*droppedSegmentGCBatchCandidate,
) {
	if len(batch) == 0 {
		return
	}

	fileStarts := make([]int, len(batch))
	fileEnds := make([]int, len(batch))
	filePaths := make([]string, 0)
	prefixes := make([]string, 0, len(batch))
	prefixCandidates := make([]int, 0, len(batch))
	for i, candidate := range batch {
		fileStarts[i] = len(filePaths)
		filePaths = append(filePaths, candidate.exactFiles...)
		fileEnds[i] = len(filePaths)
		if candidate.prefix != "" {
			prefixCandidates = append(prefixCandidates, i)
			prefixes = append(prefixes, candidate.prefix)
		}
	}

	fileErrors := make([]error, len(batch))
	if len(filePaths) > 0 {
		outcomes, batchErr := collectBatchRemoveOutcomes(filePaths, gc.removeObjectFilesWithResult(ctx, filePaths))
		for i := range batch {
			for _, filePath := range filePaths[fileStarts[i]:fileEnds[i]] {
				fileErrors[i] = merr.Combine(fileErrors[i], batchRemovePathError(filePath, outcomes, batchErr))
			}
		}
	}

	if len(prefixes) > 0 {
		outcomes, batchErr := collectBatchRemovePrefixOutcomes(
			prefixes,
			gc.option.cli.MultiRemoveWithPrefix(ctx, prefixes),
		)
		for i, prefix := range prefixes {
			candidateIndex := prefixCandidates[i]
			fileErrors[candidateIndex] = merr.Combine(
				fileErrors[candidateIndex],
				batchRemovePathError(prefix, outcomes, batchErr),
			)
		}
	}

	for i, candidate := range batch {
		if fileErrors[i] != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "failed to remove dropped segment files in batch",
				mlog.FieldSegmentID(candidate.segmentID),
				mlog.Int("exactFiles", len(candidate.exactFiles)),
				mlog.String("prefix", candidate.prefix),
				mlog.Err(fileErrors[i]))
			continue
		}
		candidate.fileDeleted = true
	}
	if ctx.Err() != nil {
		return
	}

	segmentIndexes := make([]*model.SegmentIndex, 0)
	fileComplete := make([]*droppedSegmentGCBatchCandidate, 0, len(batch))
	for _, candidate := range batch {
		if !candidate.fileDeleted {
			continue
		}
		fileComplete = append(fileComplete, candidate)
		segmentIndexes = append(segmentIndexes, candidate.segIndexes...)
	}

	if gc.meta.indexMeta != nil {
		metadataBatchSize := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
		if metadataBatchSize <= 0 {
			metadataBatchSize = 64
		}
		for start := 0; start < len(segmentIndexes); start += metadataBatchSize {
			if ctx.Err() != nil {
				break
			}
			end := min(start+metadataBatchSize, len(segmentIndexes))
			if _, err := gc.meta.indexMeta.RemoveSegmentIndexes(ctx, segmentIndexes[start:end]); err != nil {
				mlog.RatedWarn(ctx, rate.Limit(1), "failed to remove dropped segment index metadata batch",
					mlog.Int("segmentIndexes", end-start),
					mlog.Err(err))
			}
		}
	}

	segmentMetaCandidates := make([]*SegmentInfo, 0, len(fileComplete))
	for _, candidate := range fileComplete {
		allIndexesRemoved := true
		for _, segIdx := range candidate.segIndexes {
			if _, exists := gc.meta.indexMeta.GetIndexJob(segIdx.BuildID); exists {
				allIndexesRemoved = false
				break
			}
		}
		if !allIndexesRemoved {
			continue
		}
		segmentMetaCandidates = append(segmentMetaCandidates, candidate.segment)
	}
	if ctx.Err() != nil {
		return
	}

	metadataBatchSize := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if metadataBatchSize <= 0 {
		metadataBatchSize = 64
	}
	for start := 0; start < len(segmentMetaCandidates); start += metadataBatchSize {
		if ctx.Err() != nil {
			break
		}
		end := min(start+metadataBatchSize, len(segmentMetaCandidates))
		if _, err := gc.meta.DropSegments(ctx, segmentMetaCandidates[start:end]); err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "failed to remove dropped segment metadata batch",
				mlog.Int("segments", end-start),
				mlog.Err(err))
		}
	}
}

func (gc *garbageCollector) recycleDroppedSegmentsInBatches(
	ctx context.Context,
	signal <-chan gcCmd,
	drops map[int64]*SegmentInfo,
	compactTo map[int64]*SegmentInfo,
	indexedSet typeutil.UniqueSet,
	channelStates map[string]droppedSegmentGCChannelState,
	loadedSegments typeutil.Set[int64],
	channelStateBatchSize int,
) int {
	gc.loadDroppedSegmentChannelStates(ctx, signal, channelStates, channelStateBatchSize)
	if ctx.Err() != nil {
		return 0
	}

	batchSize := Params.DataCoordCfg.GCDroppedSegmentBatchSize.GetAsInt()
	if batchSize <= 0 {
		batchSize = 1000
	}
	processed := 0
	batchWeight := 0
	batch := make([]*droppedSegmentGCBatchCandidate, 0, batchSize)
	flush := func() bool {
		if len(batch) == 0 {
			gc.ackSignal(signal)
			return ctx.Err() == nil
		}

		gc.ackSignal(signal)
		active := batch[:0]
		for _, candidate := range batch {
			if !gc.collectionGCPaused(candidate.segment.GetCollectionID()) {
				active = append(active, candidate)
			}
		}
		gc.recycleDroppedSegmentBatch(ctx, active)
		batch = batch[:0]
		batchWeight = 0
		gc.ackSignal(signal)
		return ctx.Err() == nil
	}

	for segmentID, segment := range drops {
		processed++
		if ctx.Err() != nil {
			return processed
		}
		if gc.collectionGCPaused(segment.GetCollectionID()) {
			continue
		}
		state := channelStates[segment.GetInsertChannel()]
		if !gc.isDroppedSegmentGCCandidate(
			ctx,
			segmentID,
			segment,
			compactTo,
			indexedSet,
			state,
			loadedSegments,
		) {
			continue
		}

		candidate, err := gc.prepareDroppedSegmentGCBatchCandidate(ctx, segmentID, segment)
		if err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "failed to prepare dropped segment deletion batch",
				mlog.FieldSegmentID(segmentID),
				mlog.Err(err))
			continue
		}
		if candidate == nil {
			continue
		}

		candidateWeight := len(candidate.exactFiles)
		if candidate.prefix != "" {
			candidateWeight++
		}
		candidateWeight = max(1, candidateWeight)
		if len(batch) > 0 && (len(batch) >= batchSize || batchWeight+candidateWeight > batchSize) {
			if !flush() {
				return processed
			}
		}
		batch = append(batch, candidate)
		batchWeight += candidateWeight
		if batchWeight >= batchSize && !flush() {
			return processed
		}
	}
	flush()
	return processed
}

func (gc *garbageCollector) getDroppedSegmentIndexFiles(segmentID int64) ([]*model.SegmentIndex, map[string]struct{}, bool) {
	segIndexes := gc.getAllSegmentIndexesForDroppedSegment(segmentID)
	if len(segIndexes) == 0 {
		return nil, nil, false
	}
	if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil {
		for _, segIdx := range segIndexes {
			if snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
				return segIndexes, nil, true
			}
		}
	}
	indexFiles := make(map[string]struct{}, len(segIndexes))
	for _, segIdx := range segIndexes {
		for key := range gc.getAllIndexFilesOfIndex(segIdx) {
			indexFiles[key] = struct{}{}
		}
	}
	return segIndexes, indexFiles, false
}

// getAllSegmentIndexesForDroppedSegment wraps indexMeta.GetAllSegmentIndexes
// with a defensive nil guard. Production newMeta always wires indexMeta, but
// the guard is cheap and turns any unexpected nil into "no index records"
// instead of a panic during a GC sweep — keeping a single misbuilt gc
// instance from taking the whole datacoord down.
func (gc *garbageCollector) getAllSegmentIndexesForDroppedSegment(segmentID int64) []*model.SegmentIndex {
	if gc.meta == nil || gc.meta.indexMeta == nil {
		return nil
	}
	return gc.meta.indexMeta.GetAllSegmentIndexes(segmentID)
}

type droppedSegmentFilePlan struct {
	prefix     string
	exactFiles map[string]struct{}
}

func (gc *garbageCollector) buildDroppedSegmentFilePlan(
	cloned *SegmentInfo,
	indexFiles map[string]struct{},
) (droppedSegmentFilePlan, error) {
	if cloned.GetStorageVersion() == storage.StorageV3 {
		basePath, _, err := packed.UnmarshalManifestPath(cloned.GetManifestPath())
		if err != nil {
			return droppedSegmentFilePlan{}, merr.WrapErrDataIntegrity(err,
				"failed to parse StorageV3 manifest path for segment %d", cloned.GetID())
		}
		if basePath == "" {
			return droppedSegmentFilePlan{}, merr.WrapErrDataIntegrityMsg(
				"StorageV3 manifest has empty base path for segment %d", cloned.GetID())
		}
		return droppedSegmentFilePlan{prefix: basePath, exactFiles: indexFiles}, nil
	}

	binlog.DecompressBinLogs(cloned.SegmentInfo)
	logs := getLogs(cloned)
	for key := range getTextLogs(cloned) {
		logs[key] = struct{}{}
	}
	for key := range getJSONKeyLogs(cloned, gc) {
		logs[key] = struct{}{}
	}
	for key := range indexFiles {
		logs[key] = struct{}{}
	}
	return droppedSegmentFilePlan{exactFiles: logs}, nil
}

func (gc *garbageCollector) removeDroppedSegmentFiles(ctx context.Context, cloned *SegmentInfo, indexFiles map[string]struct{}) error {
	log := mlog.With(mlog.Int64("segmentID", cloned.GetID()))
	plan, err := gc.buildDroppedSegmentFilePlan(cloned, indexFiles)
	if err != nil {
		log.RatedWarn(ctx, rate.Limit(1), "GC segment failed to build file deletion plan",
			mlog.String("manifestPath", cloned.GetManifestPath()),
			mlog.Err(err))
		return err
	}

	// V3 segment data lives under the manifest base path. Segment index files still
	// live under index file prefixes and must be deleted from recorded file keys.
	if cloned.GetStorageVersion() == storage.StorageV3 {
		log.Info(ctx, "GC V3 segment start, removing basePath...",
			mlog.String("basePath", plan.prefix),
			mlog.Int("indexFiles", len(indexFiles)))
		if err := gc.option.cli.RemoveWithPrefix(ctx, plan.prefix); err != nil {
			log.Warn(ctx, "GC V3 segment remove basePath failed",
				mlog.String("basePath", plan.prefix),
				mlog.Err(err))
			return err
		}
		if len(plan.exactFiles) == 0 {
			log.Info(ctx, "GC V3 segment files done")
			return nil
		}
		if err := gc.removeObjectFiles(ctx, plan.exactFiles); err != nil {
			log.Warn(ctx, "GC V3 segment remove index files failed", mlog.Err(err))
			return err
		}
		log.Info(ctx, "GC V3 segment files done")
		return nil
	}

	log.Info(ctx, "GC segment start...", mlog.Int("insert_logs", len(cloned.GetBinlogs())),
		mlog.Int("delta_logs", len(cloned.GetDeltalogs())),
		mlog.Int("stats_logs", len(cloned.GetStatslogs())),
		mlog.Int("bm25_logs", len(cloned.GetBm25Statslogs())),
		mlog.Int("text_logs", len(cloned.GetTextStatsLogs())),
		mlog.Int("json_key_logs", len(cloned.GetJsonKeyStats())),
		mlog.Int("index_files", len(indexFiles)))
	if err := gc.removeObjectFiles(ctx, plan.exactFiles); err != nil {
		log.Warn(ctx, "GC segment remove logs failed", mlog.Err(err))
		return err
	}
	return nil
}

func (gc *garbageCollector) recycleChannelCPMeta(ctx context.Context, signal <-chan gcCmd) {
	channelCPs, err := gc.meta.catalog.ListChannelCheckpoint(ctx)
	if err != nil {
		mlog.Warn(ctx, "list channel cp fail during GC", mlog.Err(err))
		return
	}

	collectionID2GcStatus := make(map[int64]bool)
	skippedCnt := 0

	mlog.Info(ctx, "start to GC channel cp", mlog.Int("vchannelCPCnt", len(channelCPs)))
	for vChannel := range channelCPs {
		collectionID := funcutil.GetCollectionIDFromVChannel(vChannel)
		if gc.collectionGCPaused(collectionID) {
			continue
		}

		gc.ackSignal(signal)

		// !!! Skip to GC if vChannel format is illegal, it will lead meta leak in this case
		if collectionID == -1 {
			skippedCnt++
			mlog.Warn(ctx, "parse collection id fail, skip to gc channel cp", mlog.String("vchannel", vChannel))
			continue
		}

		_, ok := collectionID2GcStatus[collectionID]
		if !ok {
			if ctx.Err() != nil {
				// process canceled, stop.
				return
			}
			timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			has, err := gc.option.broker.HasCollection(timeoutCtx, collectionID)
			if err == nil && !has {
				collectionID2GcStatus[collectionID] = gc.meta.catalog.GcConfirm(ctx, collectionID, -1)
			} else {
				// skip checkpoints GC of this cycle if describe collection fails or the collection state is available.
				mlog.Debug(ctx, "skip channel cp GC, the collection state is available",
					mlog.Int64("collectionID", collectionID),
					mlog.Bool("dropped", has), mlog.Err(err))
				collectionID2GcStatus[collectionID] = false
			}
		}

		// Skip to GC if all segments meta of the corresponding collection are not removed
		if gcConfirmed := collectionID2GcStatus[collectionID]; !gcConfirmed {
			skippedCnt++
			continue
		}

		err := gc.meta.DropChannelCheckpoint(vChannel)
		if err != nil {
			// Try to GC in the next gc cycle if drop channel cp meta fail.
			mlog.Warn(ctx, "failed to drop channelcp check point during gc", mlog.String("vchannel", vChannel), mlog.Err(err))
		} else {
			mlog.Info(ctx, "GC channel cp", mlog.String("vchannel", vChannel))
		}
	}

	mlog.Info(ctx, "GC channel cp done", mlog.Int("skippedChannelCP", skippedCnt))
}

func (gc *garbageCollector) isExpire(dropts Timestamp) bool {
	droptime := time.Unix(0, int64(dropts))
	return time.Since(droptime) > gc.option.dropTolerance
}

// parseV3SegmentID attempts to parse segmentID from a V3 path format.
// V3 paths: {root}/insert_log/{coll}/{part}/{seg}/...
// Returns segmentID or error if path doesn't match.
func parseV3SegmentID(rootPath, filePath string) (int64, error) {
	if !strings.HasPrefix(filePath, rootPath) {
		return 0, merr.WrapErrServiceInternalMsg("path %q does not contain rootPath %q", filePath, rootPath)
	}
	p := strings.TrimPrefix(filePath[len(rootPath):], "/")
	parts := strings.Split(p, "/")
	if len(parts) < 5 || parts[0] != common.SegmentInsertLogPath {
		return 0, merr.WrapErrServiceInternalMsg("not a V3 insert_log path: %s", filePath)
	}
	return strconv.ParseInt(parts[3], 10, 64)
}

func parseSegmentIDFromAuxIndexPath(rootPath, filePath string) (int64, error) {
	if !strings.HasPrefix(filePath, rootPath) {
		return 0, merr.WrapErrServiceInternalMsg("path %q does not contain rootPath %q", filePath, rootPath)
	}
	p := strings.TrimPrefix(filePath[len(rootPath):], "/")
	parts := strings.Split(p, "/")
	if len(parts) < 8 || (parts[0] != common.TextIndexPath && parts[0] != common.JSONIndexPath) {
		return 0, merr.WrapErrServiceInternalMsg("not an auxiliary index path: %s", filePath)
	}
	return strconv.ParseInt(parts[5], 10, 64)
}

func parseSegmentIDFromJSONStatsPath(rootPath, filePath string) (int64, error) {
	if !strings.HasPrefix(filePath, rootPath) {
		return 0, merr.WrapErrServiceInternalMsg("path %q does not contain rootPath %q", filePath, rootPath)
	}
	p := strings.TrimPrefix(filePath[len(rootPath):], "/")
	parts := strings.Split(p, "/")
	if len(parts) < 9 || parts[0] != common.JSONStatsPath {
		return 0, merr.WrapErrServiceInternalMsg("not a json stats path: %s", filePath)
	}
	return strconv.ParseInt(parts[6], 10, 64)
}

func parseSegmentIDFromTextIndexPath(rootPath, filePath string) (int64, error) {
	return parseSegmentIDFromAuxIndexPath(rootPath, filePath)
}

func parseSegmentIDFromJSONIndexPath(rootPath, filePath string) (int64, error) {
	return parseSegmentIDFromAuxIndexPath(rootPath, filePath)
}

func getLogs(sinfo *SegmentInfo) map[string]struct{} {
	logs := make(map[string]struct{})
	for _, flog := range sinfo.GetBinlogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetStatslogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetDeltalogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetBm25Statslogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	return logs
}

func getTextLogs(sinfo *SegmentInfo) map[string]struct{} {
	return getTextLogPaths(sinfo, "")
}

func getTextLogPaths(sinfo *SegmentInfo, rootPath string) map[string]struct{} {
	textLogs := make(map[string]struct{})
	for _, flog := range sinfo.GetTextStatsLogs() {
		files := flog.GetFiles()
		if rootPath != "" {
			basePath := metautil.BuildTextIndexPrefix(rootPath,
				flog.GetBuildID(), flog.GetVersion(),
				sinfo.GetCollectionID(), sinfo.GetPartitionID(), sinfo.GetID(), flog.GetFieldID())
			files = metautil.BuildStatsFilePaths(basePath, files)
		}
		for _, file := range files {
			textLogs[file] = struct{}{}
		}
	}

	return textLogs
}

func getJSONKeyLogs(sinfo *SegmentInfo, gc *garbageCollector) map[string]struct{} {
	jsonkeyLogs := make(map[string]struct{})
	for _, flog := range sinfo.GetJsonKeyStats() {
		for _, file := range flog.GetFiles() {
			var prefix string
			if flog.GetJsonKeyStatsDataFormat() >= 2 {
				prefix = metautil.BuildJSONKeyStatsPrefix(
					gc.option.cli.RootPath(),
					flog.GetJsonKeyStatsDataFormat(),
					flog.GetBuildID(),
					flog.GetVersion(),
					sinfo.GetCollectionID(),
					sinfo.GetPartitionID(),
					sinfo.GetID(),
					flog.GetFieldID(),
				)
			} else {
				prefix = fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.JSONIndexPath,
					flog.GetBuildID(), flog.GetVersion(), sinfo.GetCollectionID(), sinfo.GetPartitionID(), sinfo.GetID(), flog.GetFieldID())
			}
			file = path.Join(prefix, file)
			jsonkeyLogs[file] = struct{}{}
		}
	}

	return jsonkeyLogs
}

// removeObjectFiles remove file from oss storage, return error if any log failed to remove.
func (gc *garbageCollector) removeObjectFiles(ctx context.Context, filePaths map[string]struct{}) error {
	futures := make([]*conc.Future[struct{}], 0)
	for filePath := range filePaths {
		filePath := filePath
		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			err := gc.option.cli.Remove(ctx, filePath)
			// ignore the error Key Not Found
			if err != nil {
				if !errors.Is(err, merr.ErrIoKeyNotFound) {
					return struct{}{}, err
				}
				mlog.Info(ctx, "remove log failed, key not found, may be removed at previous GC, ignore the error",
					mlog.String("path", filePath),
					mlog.Err(err))
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	return conc.BlockOnAll(futures...)
}

type segmentIndexGCCandidate struct {
	segmentIndex *model.SegmentIndex
	fileStart    int
	fileEnd      int
}

type removeOutcome struct {
	seen bool
	err  error
}

// recycleUnusedSegIndexBatch validates a bounded set of SegmentIndexes, removes
// all candidate files through one batch GC stage, then removes metadata only
// for build IDs whose complete file set succeeded. Storage uses a native batch
// capability when available and bounded per-path deletion otherwise. Metadata
// uses a bounded catalog batch when available and keeps the per-build
// publication path otherwise.
func (gc *garbageCollector) recycleUnusedSegIndexBatch(
	ctx context.Context,
	batch []*model.SegmentIndex,
) {
	candidates := make([]segmentIndexGCCandidate, 0, len(batch))
	filePaths := make([]string, 0, len(batch))
	log := mlog.With(mlog.String("gcName", "recycleUnusedSegIndexes"))

	for _, candidate := range batch {
		if ctx.Err() != nil {
			return
		}

		segIdx, ok := gc.getLatestSegmentIndexForGC(candidate)
		if !ok {
			continue
		}
		if gc.collectionGCPaused(segIdx.CollectionID) {
			continue
		}
		if gc.meta.GetSegment(ctx, segIdx.SegmentID) != nil && gc.meta.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			continue
		}

		if segIdx.IndexState == commonpb.IndexState_Unissued ||
			segIdx.IndexState == commonpb.IndexState_InProgress ||
			segIdx.IndexState == commonpb.IndexState_Retry {
			continue
		}

		if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil &&
			snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
			continue
		}

		fileStart := len(filePaths)
		filePaths = gc.appendIndexFilesOfIndex(filePaths, segIdx)
		candidates = append(candidates, segmentIndexGCCandidate{
			segmentIndex: segIdx,
			fileStart:    fileStart,
			fileEnd:      len(filePaths),
		})
	}

	outcomes := make(map[string]removeOutcome, len(filePaths))
	var batchErr error
	if len(filePaths) > 0 {
		outcomes, batchErr = collectBatchRemoveOutcomes(filePaths, gc.removeObjectFilesWithResult(ctx, filePaths))
	}

	metadataCandidates := make([]*model.SegmentIndex, 0, len(candidates))
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return
		}

		var fileErr error
		candidateFiles := filePaths[candidate.fileStart:candidate.fileEnd]
		for _, filePath := range candidateFiles {
			fileErr = merr.Combine(fileErr, batchRemovePathError(filePath, outcomes, batchErr))
		}
		if fileErr != nil {
			log.RatedWarn(ctx, rate.Limit(1), "failed to remove segment index files in batch",
				mlog.FieldBuildID(candidate.segmentIndex.BuildID),
				mlog.Int("indexFiles", len(candidateFiles)),
				mlog.Err(fileErr))
			continue
		}
		metadataCandidates = append(metadataCandidates, candidate.segmentIndex)
	}

	metadataBatchSize := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if metadataBatchSize <= 0 {
		metadataBatchSize = 64
	}
	for start := 0; start < len(metadataCandidates); start += metadataBatchSize {
		if ctx.Err() != nil {
			break
		}
		end := min(start+metadataBatchSize, len(metadataCandidates))
		batch := metadataCandidates[start:end]
		_, err := gc.meta.indexMeta.RemoveSegmentIndexes(ctx, batch)
		if err != nil {
			log.RatedWarn(ctx, rate.Limit(1), "failed to remove segment index metadata batch",
				mlog.Int("segmentIndexes", len(batch)),
				mlog.Err(err))
			continue
		}
	}
}

func (gc *garbageCollector) recycleUnusedSegIndexesInBatches(
	ctx context.Context,
	signal <-chan gcCmd,
) {
	batchSize := Params.DataCoordCfg.GCIndexFileBatchSize.GetAsInt()
	if batchSize <= 0 {
		batchSize = 1000
	}

	batch := make([]*model.SegmentIndex, 0, batchSize)
	batchFileEstimate := 0
	flush := func() bool {
		if len(batch) == 0 {
			gc.ackSignal(signal)
			return ctx.Err() == nil
		}

		// A pause record is inserted before its signal is sent. Receiving the
		// signal here guarantees previously admitted work is complete; candidate
		// validation below observes the new pause record before deleting files.
		gc.ackSignal(signal)
		gc.recycleUnusedSegIndexBatch(ctx, batch)
		batch = batch[:0]
		batchFileEstimate = 0
		gc.ackSignal(signal)
		return ctx.Err() == nil
	}

	gc.meta.indexMeta.RangeSegmentIndexes(func(candidate *model.SegmentIndex) bool {
		if ctx.Err() != nil {
			return false
		}
		candidateWeight := 1
		if candidate != nil && len(candidate.IndexFileKeys) > 0 {
			candidateWeight = len(candidate.IndexFileKeys)
		}
		if len(batch) > 0 && (len(batch) >= batchSize || batchFileEstimate+candidateWeight > batchSize) && !flush() {
			return false
		}
		batch = append(batch, candidate)
		batchFileEstimate += candidateWeight
		if (len(batch) >= batchSize || batchFileEstimate >= batchSize) && !flush() {
			return false
		}
		return true
	})
	if ctx.Err() == nil {
		flush()
	}
}

type fieldIndexGCBatchStats struct {
	scanned       int
	paused        int
	deleteSuccess int
	deleteFailed  int
	deleteSkipped int
	batchCount    int
	deleteTime    time.Duration
}

func (gc *garbageCollector) recycleUnusedIndexesInBatches(
	ctx context.Context,
	signal <-chan gcCmd,
	deletedIndexes []*model.Index,
) {
	batchSize := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if batchSize <= 0 {
		batchSize = 64
	}

	stats := fieldIndexGCBatchStats{scanned: len(deletedIndexes)}
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexes"))
	for start := 0; start < len(deletedIndexes); start += batchSize {
		if ctx.Err() != nil {
			break
		}

		// Pause records are installed before their signal is sent. Ack first so a
		// successful pause means all earlier work completed, then filter the next
		// bounded batch against the newly visible records.
		gc.ackSignal(signal)
		end := min(start+batchSize, len(deletedIndexes))
		candidates := make([]*model.Index, 0, end-start)
		for _, index := range deletedIndexes[start:end] {
			if index == nil {
				continue
			}
			if gc.collectionGCPaused(index.CollectionID) {
				stats.paused++
				continue
			}
			candidates = append(candidates, index)
		}
		if len(candidates) == 0 {
			continue
		}

		stats.batchCount++
		deleteStart := time.Now()
		removed, err := gc.meta.indexMeta.RemoveIndexes(ctx, candidates)
		stats.deleteTime += time.Since(deleteStart)
		if err != nil {
			stats.deleteFailed += len(candidates)
			log.RatedWarn(ctx, rate.Limit(1), "remove field-index metadata batch failed",
				mlog.Int("indexes", len(candidates)),
				mlog.Err(err))
			continue
		}
		stats.deleteSuccess += removed
		stats.deleteSkipped += len(candidates) - removed
	}
	gc.ackSignal(signal)

	log.Info(ctx, "recycle unused field indexes batch summary",
		mlog.Int("scanned", stats.scanned),
		mlog.Int("paused", stats.paused),
		mlog.Int("deleteSuccess", stats.deleteSuccess),
		mlog.Int("deleteFailed", stats.deleteFailed),
		mlog.Int("deleteSkipped", stats.deleteSkipped),
		mlog.Int("batchCount", stats.batchCount),
		mlog.Duration("deleteTime", stats.deleteTime))
}

// recycleUnusedIndexes is used to delete those indexes that is deleted by collection.
func (gc *garbageCollector) recycleUnusedIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexes"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedIndexes done", mlog.Duration("timeCost", time.Since(start)))
	}()

	deletedIndexes := gc.meta.indexMeta.GetDeletedIndexes()
	gc.recycleUnusedIndexesInBatches(ctx, signal, deletedIndexes)
}

// recycleUnusedSegIndexes remove the index of segment if index is deleted or segment itself is deleted.
func (gc *garbageCollector) recycleUnusedSegIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedSegIndexes"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedSegIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedSegIndexes done", mlog.Duration("timeCost", time.Since(start)))
	}()

	gc.recycleUnusedSegIndexesInBatches(ctx, signal)
}

// getLatestSegmentIndexForGC takes a SegmentIndex candidate from GC scanning and
// returns the latest SegmentIndex meta for the same buildID. The bool return
// value is false when the candidate is nil or the buildID no longer exists.
func (gc *garbageCollector) getLatestSegmentIndexForGC(candidate *model.SegmentIndex) (*model.SegmentIndex, bool) {
	if candidate == nil {
		return nil, false
	}
	if gc.meta == nil || gc.meta.indexMeta == nil || gc.meta.indexMeta.segmentBuildInfo == nil {
		return candidate, true
	}
	return gc.meta.indexMeta.GetIndexJob(candidate.BuildID)
}

// recycleUnusedIndexFilesV0 deletes orphan files under the legacy v0 index_files prefix.
// v0 paths are rooted by buildID, so the first-level directory can be parsed and
// checked against index meta directly.
func (gc *garbageCollector) recycleUnusedIndexFilesV0(ctx context.Context) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexFilesV0"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedIndexFilesV0...")

	prefix := path.Join(gc.option.cli.RootPath(), common.SegmentIndexV0Path) + "/"

	// Resolve snapshotMeta once. Both IsBuildIDGCBlocked paths below are O(1) so
	// no caching of intermediate state is needed.
	snapshotMeta := gc.meta.GetSnapshotMeta()

	// list dir first
	keyCount := 0
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(indexPathInfo *storage.ChunkObjectInfo) bool {
		key := indexPathInfo.FilePath
		keyCount++
		logger := mlog.With(mlog.String("prefix", prefix), mlog.String("key", key))

		// This recycler only walks index_files/ (v0). Its first path level is buildID;
		// v1 collectionID directories live under index_v1/ and are handled below.
		buildID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			logger.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 parseIndexFileKey", mlog.Err(err))
			return true
		}
		logger = logger.With(mlog.Int64("buildID", buildID))
		logger.Info(ctx, "garbageCollector will recycle index files")
		canRecycle, segIdx := gc.meta.indexMeta.CheckCleanSegmentIndex(buildID)
		if !canRecycle {
			// Even if the index is marked as deleted, the index file will not be recycled, wait for the next gc,
			// and delete all index files about the buildID at one time.
			logger.Info(ctx, "garbageCollector can not recycle index files")
			return true
		}
		if segIdx == nil {
			// buildID no longer exists in meta. Orphan buildID walk has no collection context,
			// so IsBuildIDGCBlocked(-1, buildID) fail-closes on ANY unloaded RefIndex globally.
			if snapshotMeta != nil && snapshotMeta.IsBuildIDGCBlocked(-1, buildID) {
				logger.Info(ctx, "skip GC index files since buildID is protected by snapshot",
					mlog.Int64("buildID", buildID))
				return true
			}

			// buildID no longer exists in meta, remove all index files
			logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 find meta has not exist, remove index files")
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				logger.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove index files failed", mlog.Err(err))
				return true
			}
			logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove index files success")
			return true
		}

		// Skip buildIDs protected by snapshot references. IsBuildIDGCBlocked is O(1)
		// and embeds the "RefIndex not loaded → fail-closed" check.
		if snapshotMeta != nil {
			if snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
				logger.Info(ctx, "skip GC index files since buildID is protected by snapshot",
					mlog.Int64("collectionID", segIdx.CollectionID),
					mlog.Int64("buildID", segIdx.BuildID))
				return true
			}
		}

		filesMap := gc.getAllIndexFilesOfIndex(segIdx)

		logger.Info(ctx, "recycle index files", mlog.Int("meta files num", len(filesMap)))
		deletedFilesNum := atomic.NewInt32(0)
		fileNum := 0

		futures := make([]*conc.Future[struct{}], 0)
		err = gc.option.cli.WalkWithPrefix(ctx, key, true, func(indexFile *storage.ChunkObjectInfo) bool {
			fileNum++
			file := indexFile.FilePath
			if _, ok := filesMap[file]; !ok {
				future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
					logger := logger.With(mlog.String("file", file))
					logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove file...")

					if err := gc.option.cli.Remove(ctx, file); err != nil {
						logger.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove file failed", mlog.Err(err))
						return struct{}{}, err
					}
					deletedFilesNum.Inc()
					logger.Info(ctx, "garbageCollector recycleUnusedIndexFilesV0 remove file success")
					return struct{}{}, nil
				})
				futures = append(futures, future)
			}
			return true
		})
		// Wait for all remove tasks done.
		if err := conc.BlockOnAll(futures...); err != nil {
			// error is logged, and can be ignored here.
			logger.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
		}

		logger = logger.With(mlog.Int("deleteIndexFilesNum", int(deletedFilesNum.Load())), mlog.Int("walkFileNum", fileNum))
		if err != nil {
			logger.Warn(ctx, "index files recycle failed when walk with prefix", mlog.Err(err))
			return true
		}
		logger.Info(ctx, "index files recycle done")
		return true
	})
	log = log.With(mlog.Duration("timeCost", time.Since(start)), mlog.Int("keyCount", keyCount), mlog.Err(err))
	if err != nil {
		log.Warn(ctx, "garbageCollector recycleUnusedIndexFilesV0 failed", mlog.Err(err))
		return
	}
	log.Info(ctx, "recycleUnusedIndexFilesV0 done")
}

// getAllIndexFilesOfIndex returns all expected index files using the path version
// recorded on the SegmentIndex: v0 builds index_files paths, v1 builds index_v1 paths.
func (gc *garbageCollector) getAllIndexFilesOfIndex(segmentIndex *model.SegmentIndex) map[string]struct{} {
	files := gc.appendIndexFilesOfIndex(nil, segmentIndex)
	filesMap := make(map[string]struct{}, len(files))
	for _, file := range files {
		filesMap[file] = struct{}{}
	}
	return filesMap
}

func (gc *garbageCollector) appendIndexFilesOfIndex(dst []string, segmentIndex *model.SegmentIndex) []string {
	builder := metautil.NewIndexPathBuilder(gc.option.cli.RootPath(),
		segmentIndex.IndexStorePathVersion, segmentIndex.CollectionID,
		segmentIndex.PartitionID, segmentIndex.SegmentID,
		segmentIndex.BuildID, segmentIndex.IndexVersion)
	for _, fileID := range segmentIndex.IndexFileKeys {
		dst = append(dst, builder.BuildFilePath(fileID))
	}
	return dst
}

// recycleUnusedIndexFilesV1 cleans index files for v1 format entries (collection-partitioned paths).
// v1 uses the separate index_v1 prefix and puts collectionID before buildID,
// so GC iterates deleted metadata entries instead of trying to parse buildID from a prefix walk.
func (gc *garbageCollector) recycleUnusedIndexFilesV1(ctx context.Context) {
	log := mlog.With(mlog.String("gcName", "recycleUnusedIndexFilesV1"))

	snapshotMeta := gc.meta.GetSnapshotMeta()
	deletedIndexes := gc.meta.indexMeta.GetDeletedIndexesWithV1Path()
	if len(deletedIndexes) == 0 {
		return
	}

	log.Info(ctx, "start recycleUnusedIndexFilesV1", mlog.Int("deletedCount", len(deletedIndexes)))
	futures := make([]*conc.Future[struct{}], 0, len(deletedIndexes))
	for _, segIdx := range deletedIndexes {
		segIdx := segIdx
		if snapshotMeta != nil && snapshotMeta.IsBuildIDGCBlocked(segIdx.CollectionID, segIdx.BuildID) {
			log.Info(ctx, "skip GC v1 index files since buildID is protected by snapshot",
				mlog.Int64("collectionID", segIdx.CollectionID),
				mlog.Int64("buildID", segIdx.BuildID))
			continue
		}

		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			builder := metautil.NewIndexPathBuilder(gc.option.cli.RootPath(),
				segIdx.IndexStorePathVersion, segIdx.CollectionID,
				segIdx.PartitionID, segIdx.SegmentID,
				segIdx.BuildID, segIdx.IndexVersion)
			prefix := builder.BuildPrefix() + "/"

			if err := gc.option.cli.RemoveWithPrefix(ctx, prefix); err != nil {
				log.Warn(ctx, "recycleUnusedIndexFilesV1 remove failed",
					mlog.Int64("collectionID", segIdx.CollectionID),
					mlog.Int64("partitionID", segIdx.PartitionID),
					mlog.Int64("segmentID", segIdx.SegmentID),
					mlog.Int64("buildID", segIdx.BuildID),
					mlog.Int64("indexID", segIdx.IndexID),
					mlog.Stringer("pathVersion", segIdx.IndexStorePathVersion),
					mlog.String("prefix", prefix),
					mlog.Err(err))
				return struct{}{}, err
			}
			if err := gc.meta.indexMeta.RemoveSegmentIndex(ctx, segIdx.BuildID); err != nil {
				log.Warn(ctx, "recycleUnusedIndexFilesV1 remove segment index meta failed",
					mlog.Int64("collectionID", segIdx.CollectionID),
					mlog.Int64("partitionID", segIdx.PartitionID),
					mlog.Int64("segmentID", segIdx.SegmentID),
					mlog.Int64("buildID", segIdx.BuildID),
					mlog.Int64("indexID", segIdx.IndexID),
					mlog.Stringer("pathVersion", segIdx.IndexStorePathVersion),
					mlog.String("prefix", prefix),
					mlog.Err(err))
				return struct{}{}, err
			}
			log.Info(ctx, "recycleUnusedIndexFilesV1 removed index files and meta",
				mlog.Int64("collectionID", segIdx.CollectionID),
				mlog.Int64("partitionID", segIdx.PartitionID),
				mlog.Int64("segmentID", segIdx.SegmentID),
				mlog.Int64("buildID", segIdx.BuildID),
				mlog.Int64("indexID", segIdx.IndexID),
				mlog.Stringer("pathVersion", segIdx.IndexStorePathVersion),
				mlog.String("prefix", prefix))
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	if err := conc.BlockOnAll(futures...); err != nil {
		log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
	}
}

// recycleUnusedAnalyzeFiles is used to delete those analyze stats files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedAnalyzeFiles(ctx context.Context, signal <-chan gcCmd) {
	mlog.Info(ctx, "start recycleUnusedAnalyzeFiles")
	startTs := time.Now()
	prefix := path.Join(gc.option.cli.RootPath(), common.AnalyzeStatsPath) + "/"
	// list dir first
	keys := make([]string, 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(chunkInfo *storage.ChunkObjectInfo) bool {
		keys = append(keys, chunkInfo.FilePath)
		return true
	})
	if err != nil {
		mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles list keys from chunk manager failed", mlog.Err(err))
		return
	}
	mlog.Info(ctx, "recycleUnusedAnalyzeFiles, finish list object", mlog.Duration("time spent", time.Since(startTs)), mlog.Int("task ids", len(keys)))
	for _, key := range keys {
		if ctx.Err() != nil {
			// process canceled
			return
		}
		// collection gc pause not affect analyze file for now
		gc.ackSignal(signal)

		mlog.Debug(ctx, "analyze keys", mlog.String("key", key))
		taskID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles parseAnalyzeResult failed", mlog.String("key", key), mlog.Err(err))
			continue
		}
		mlog.Info(ctx, "garbageCollector will recycle analyze stats files", mlog.Int64("taskID", taskID))
		canRecycle, task := gc.meta.analyzeMeta.CheckCleanAnalyzeTask(taskID)
		if !canRecycle {
			// Even if the analysis task is marked as deleted, the analysis stats file will not be recycled, wait for the next gc,
			// and delete all index files about the taskID at one time.
			mlog.Info(ctx, "garbageCollector no need to recycle analyze stats files", mlog.Int64("taskID", taskID))
			continue
		}
		if task == nil {
			// taskID no longer exists in meta, remove all analysis files
			mlog.Info(ctx, "garbageCollector recycleUnusedAnalyzeFiles find meta has not exist, remove index files",
				mlog.Int64("taskID", taskID))
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files failed",
					mlog.Int64("taskID", taskID), mlog.String("prefix", key), mlog.Err(err))
				continue
			}
			mlog.Info(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files success",
				mlog.Int64("taskID", taskID), mlog.String("prefix", key))
			continue
		}

		mlog.Info(ctx, "remove analyze stats files which version is less than current task",
			mlog.Int64("taskID", taskID), mlog.Int64("current version", task.Version))
		var i int64
		for i = 0; i < task.Version; i++ {
			if ctx.Err() != nil {
				// process canceled.
				return
			}
			// analyze stats files are laid out as analyze_stats/{taskID}/{version}/...
			removePrefix := prefix + fmt.Sprintf("%d/%d/", taskID, i)
			if err := gc.option.cli.RemoveWithPrefix(ctx, removePrefix); err != nil {
				mlog.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove files with prefix failed",
					mlog.Int64("taskID", taskID), mlog.String("removePrefix", removePrefix))
				continue
			}
		}
		mlog.Info(ctx, "analyze stats files recycle success", mlog.Int64("taskID", taskID))
	}
}

// recycleUnusedTextIndexFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedTextIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedTextIndexFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedTextIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedTextIndexFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasTextIndexSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetTextStatsLogs()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	snapshotMeta := gc.meta.GetSnapshotMeta()

	for _, seg := range hasTextIndexSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", mlog.Int64("segmentID", seg.GetID()), mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Skip segments whose files are still referenced by snapshots. IsSegmentGCBlocked
		// is O(1) and embeds the "RefIndex not loaded → fail-closed" check.
		if snapshotMeta != nil && snapshotMeta.IsSegmentGCBlocked(seg.GetCollectionID(), seg.GetID()) {
			log.Info(ctx, "skip GC text index files since segment is protected by snapshot",
				mlog.Int64("segmentID", seg.GetID()),
				mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetTextStatsLogs() {
			log := mlog.With(mlog.Int64("segmentID", seg.GetID()), mlog.Int64("fieldID", fieldStats.GetFieldID()))
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := metautil.BuildTextIndexPrefix(gc.option.cli.RootPath(),
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				log = log.With(mlog.Int("deleteIndexFilesNum", int(deletedFilesNum.Load())), mlog.Int("walkFileNum", fileNum))
				if err != nil {
					log.Warn(ctx, "text index files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "text index files recycle done")

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedJSONStatsFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedJSONStatsFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedJSONStatsFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedJSONStatsFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONStatsFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasJSONStatsSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetJsonKeyStats()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	snapshotMeta := gc.meta.GetSnapshotMeta()

	for _, seg := range hasJSONStatsSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", mlog.Int64("segmentID", seg.GetID()), mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Skip segments whose files are still referenced by snapshots.
		if snapshotMeta != nil && snapshotMeta.IsSegmentGCBlocked(seg.GetCollectionID(), seg.GetID()) {
			log.Info(ctx, "skip GC JSON stats files since segment is protected by snapshot",
				mlog.Int64("segmentID", seg.GetID()),
				mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetJsonKeyStats() {
			log := mlog.With(mlog.Int64("segmentID", seg.GetID()), mlog.Int64("fieldID", fieldStats.GetFieldID()))
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := metautil.BuildJSONKeyStatsPrefix(gc.option.cli.RootPath(), fieldStats.GetJsonKeyStatsDataFormat(),
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json stats files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}

			// clear low data format version stats file
			// for upgrade from old version to new version, we need to clear the old data format version stats file
			for i := int64(1); i < fieldStats.GetJsonKeyStatsDataFormat(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d", gc.option.cli.RootPath(), common.JSONStatsPath, i)
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json stats lower data format files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "json stats files recycle done",
		mlog.Int("deleteJSONStatsNum", int(deletedFilesNum.Load())),
		mlog.Int("walkFileNum", fileNum))

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedJSONIndexFiles load meta file info and compares OSS keys
func (gc *garbageCollector) recycleUnusedJSONIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleUnusedJSONIndexFiles"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleUnusedJSONIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONIndexFiles done", mlog.Duration("timeCost", time.Since(start)))
	}()

	hasJSONIndexSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetJsonKeyStats()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	for _, seg := range hasJSONIndexSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", mlog.Int64("segmentID", seg.GetID()), mlog.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Skip segments whose files are still referenced by snapshots.
		if snapshotMeta := gc.meta.GetSnapshotMeta(); snapshotMeta != nil {
			if snapshotMeta.IsSegmentGCBlocked(seg.GetCollectionID(), seg.GetID()) {
				log.Info(ctx, "skip GC JSON index files since segment is protected by snapshot",
					mlog.Int64("segmentID", seg.GetID()),
					mlog.Int64("collectionID", seg.GetCollectionID()))
				continue
			}
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetJsonKeyStats() {
			log := mlog.With(mlog.Int64("segmentID", seg.GetID()), mlog.Int64("fieldID", fieldStats.GetFieldID()))
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.JSONIndexPath,
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					fileNum++
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log := mlog.With(mlog.String("file", file))
						log.Info(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file failed", mlog.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", mlog.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json index files recycle failed when walk with prefix", mlog.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "json index files recycle done", mlog.Int("deleteJSONKeyIndexNum", int(deletedFilesNum.Load())), mlog.Int("walkFileNum", fileNum))

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleSnapshots cleans up snapshot resources in three phases:
//  1. PENDING snapshots: Failed 2PC commits that exceeded timeout — clean S3 + catalog.
//  2. DELETING snapshots: DropSnapshot succeeded but S3 cleanup failed — retry S3 + catalog.
//  3. Orphan snapshots: Snapshots whose collection was dropped — clean expired pins, then drop.
//
// Process flow:
//  1. Get all PENDING snapshots from catalog that have exceeded timeout.
//  2. For each pending snapshot:
//     a. Compute manifest directory and metadata file path from snapshot ID.
//     b. Delete manifest directory using RemoveWithPrefix.
//     c. Delete metadata file.
//     d. Delete catalog (etcd) record.
//
// Failure handling:
//   - For PENDING snapshots, if any S3 cleanup step fails (b/c), GC will NOT
//     delete the catalog record. This keeps the snapshot eligible for retry in
//     the next GC cycle, ensuring we do not lose the ability to clean up S3
//     artifacts.
func (gc *garbageCollector) recycleSnapshots(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log := mlog.With(mlog.String("gcName", "recycleSnapshots"), mlog.Time("startAt", start))
	log.Info(ctx, "start recycleSnapshots...")
	defer func() {
		log.Info(ctx, "recycleSnapshots done", mlog.Duration("timeCost", time.Since(start)))
	}()

	snapshotMeta := gc.meta.GetSnapshotMeta()
	if snapshotMeta == nil {
		log.Warn(ctx, "snapshotMeta is nil, skip recycleSnapshots")
		return
	}

	// Get pending timeout from config
	pendingTimeout := paramtable.Get().DataCoordCfg.SnapshotPendingTimeout.GetAsDuration(time.Minute)

	// Get all pending snapshots that have exceeded timeout
	pendingSnapshots, err := snapshotMeta.GetPendingSnapshots(ctx, pendingTimeout)
	if err != nil {
		log.Warn(ctx, "failed to get pending snapshots", mlog.Err(err))
		return
	}

	if len(pendingSnapshots) > 0 {
		log.Info(ctx, "found pending snapshots to cleanup", mlog.Int("count", len(pendingSnapshots)))
		cleanedCount := 0

		for _, snapshot := range pendingSnapshots {
			snapshotLog := mlog.With(
				mlog.String("snapshotName", snapshot.GetName()),
				mlog.Int64("snapshotID", snapshot.GetId()),
				mlog.Int64("collectionID", snapshot.GetCollectionId()),
			)

			gc.ackSignal(signal)
			// Compute paths from collection_id + snapshot_id
			manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(
				gc.option.cli.RootPath(),
				snapshot.GetCollectionId(),
				snapshot.GetId(),
			)

			snapshotLog.Info(ctx, "cleaning up pending snapshot",
				mlog.String("manifestDir", manifestDir),
				mlog.String("metadataPath", metadataPath))

			// Delete manifest directory using RemoveWithPrefix (no list needed)
			// This removes all segment manifest files: manifests/{snapshot_id}/*.avro
			if err := gc.option.cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
				snapshotLog.Warn(ctx, "failed to remove pending snapshot manifest directory", mlog.Err(err))
				// Keep catalog record for retry in next GC cycle.
				continue
			}

			// Delete metadata file
			if err := gc.option.cli.Remove(ctx, metadataPath); err != nil {
				snapshotLog.Warn(ctx, "failed to remove pending snapshot metadata file", mlog.Err(err))
				// Keep catalog record for retry in next GC cycle.
				continue
			}

			// Delete etcd record
			if err := snapshotMeta.CleanupPendingSnapshot(ctx, snapshot); err != nil {
				snapshotLog.Warn(ctx, "failed to drop pending snapshot from catalog", mlog.Err(err))
				continue
			}

			snapshotLog.Info(ctx, "successfully cleaned up pending snapshot")
			cleanedCount++
		}

		log.Info(ctx, "pending snapshots cleanup completed",
			mlog.Int("totalPending", len(pendingSnapshots)),
			mlog.Int("cleanedCount", cleanedCount))
	}

	// Clean up DELETING snapshots (two-phase delete cleanup)
	// These are snapshots that were marked for deletion but S3 cleanup failed
	deletingSnapshots, err := snapshotMeta.GetDeletingSnapshots(ctx)
	if err != nil {
		log.Warn(ctx, "failed to get deleting snapshots", mlog.Err(err))
	} else if len(deletingSnapshots) > 0 {
		log.Info(ctx, "found deleting snapshots to cleanup", mlog.Int("count", len(deletingSnapshots)))
		deletingCleanedCount := 0

		for _, snapshot := range deletingSnapshots {
			snapshotLog := mlog.With(
				mlog.String("snapshotName", snapshot.GetName()),
				mlog.Int64("snapshotID", snapshot.GetId()),
				mlog.Int64("collectionID", snapshot.GetCollectionId()),
			)

			gc.ackSignal(signal)

			// Compute paths from collection_id + snapshot_id
			manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(
				gc.option.cli.RootPath(),
				snapshot.GetCollectionId(),
				snapshot.GetId(),
			)

			snapshotLog.Info(ctx, "cleaning up deleting snapshot",
				mlog.String("manifestDir", manifestDir),
				mlog.String("metadataPath", metadataPath))

			// Delete manifest directory
			if err := gc.option.cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot manifest directory", mlog.Err(err))
				// Continue with metadata and etcd cleanup even if S3 cleanup fails
			}

			// Delete metadata file
			if err := gc.option.cli.Remove(ctx, metadataPath); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot metadata file", mlog.Err(err))
				// Continue with etcd cleanup even if S3 cleanup fails
			}

			// Delete etcd record
			if err := snapshotMeta.CleanupDeletingSnapshot(ctx, snapshot); err != nil {
				snapshotLog.Warn(ctx, "failed to drop deleting snapshot from catalog", mlog.Err(err))
				continue
			}

			snapshotLog.Info(ctx, "successfully cleaned up deleting snapshot")
			deletingCleanedCount++
		}

		log.Info(ctx, "deleting snapshots cleanup completed",
			mlog.Int("totalDeleting", len(deletingSnapshots)),
			mlog.Int("cleanedCount", deletingCleanedCount))
	}

	// GC fallback: Two responsibilities per collection:
	//   1. For EVERY collection with snapshot records, reap expired pin entries
	//      from SnapshotInfo to bound etcd storage growth. Orphan pins
	//      (crashed restores, swallowed Unpin errors) would otherwise accumulate
	//      forever since Pin/Unpin only touch their own entries.
	//   2. For collections whose owning collection was DROPPED, cascade-delete
	//      the orphan snapshots. Handles the case where the drop-collection
	//      cascade callback failed to fully clean up.
	activeCollectionIDs := snapshotMeta.GetActiveCollectionIDs()

	if len(activeCollectionIDs) > 0 {
		orphanCleanedCount := 0
		for _, collectionID := range activeCollectionIDs {
			gc.ackSignal(signal)

			if ctx.Err() != nil {
				log.Warn(ctx, "context canceled, stop snapshot cleanup")
				break
			}

			// Step 1: reap expired pins regardless of collection liveness.
			for _, r := range snapshotMeta.cleanExpiredPinsForCollection(ctx, collectionID) {
				setSnapshotActivePinsGauge(r.CollectionID, r.SnapshotName, r.ActivePins)
			}

			// Step 2: if the collection was dropped, cascade-delete orphan snapshots.
			timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			has, err := gc.option.broker.HasCollection(timeoutCtx, collectionID)
			cancel()
			if err != nil {
				log.Warn(ctx, "failed to check collection existence for orphan snapshot cleanup",
					mlog.Int64("collectionID", collectionID),
					mlog.Err(err))
				continue
			}
			if has {
				// Collection still exists, not an orphan — expired pins already reaped above.
				continue
			}

			log.Info(ctx, "found orphan snapshots for dropped collection, cleaning up",
				mlog.Int64("collectionID", collectionID))

			dropped, err := snapshotMeta.DropSnapshotsByCollection(ctx, collectionID)
			for _, n := range dropped {
				setSnapshotActivePinsGauge(collectionID, n, 0)
			}
			if err != nil {
				log.Warn(ctx, "failed to drop orphan snapshots for collection",
					mlog.Int64("collectionID", collectionID),
					mlog.Err(err))
				continue
			}

			log.Info(ctx, "successfully cleaned up orphan snapshots for dropped collection",
				mlog.Int64("collectionID", collectionID))
			orphanCleanedCount++
		}

		if orphanCleanedCount > 0 {
			log.Info(ctx, "orphan snapshots cleanup completed",
				mlog.Int("totalOrphanCollections", len(activeCollectionIDs)),
				mlog.Int("cleanedCount", orphanCleanedCount))
		}
	}

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}
