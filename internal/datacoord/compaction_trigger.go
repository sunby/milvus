package datacoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

const (
	signalBufferSize               = 100
	maxLittleSegmentNum            = 10
	maxCompactionTimeoutInSeconds  = 60
	singleCompactionRatioThreshold = 0.2
)

type timetravel struct {
	time Timestamp
}

type trigger interface {
	// triggerCompaction trigger a compaction if any compaction condition satisfy.
	triggerCompaction(timetravel *timetravel)
	// triggerSingleCompaction trigerr a compaction bundled with collection-partiiton-channel-segment
	triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, timetravel *timetravel)
	// forceTriggerCompaction force to start a compaction
	forceTriggerCompaction(collectionID int64, timetravel *timetravel)
}

type compactionSignal struct {
	isForce      bool
	isGlobal     bool
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	channel      string
	timetravel   *timetravel
}

type compactionTrigger struct {
	meta                   *meta
	allocator              allocator
	signals                chan *compactionSignal
	singleCompactionPolicy singleCompactionPolicy
	mergeCompactionPolicy  mergeCompactionPolicy
	compactionHandler      compactionPlanContext
}

func newCompactionTrigger(meta *meta, compactionHandler *compactionPlanHandler) *compactionTrigger {
	return &compactionTrigger{
		meta:                  meta,
		signals:               make(chan *compactionSignal, signalBufferSize),
		mergeCompactionPolicy: (mergeCompactionFunc)(greedyMergeCompaction),
		compactionHandler:     compactionHandler,
	}
}

func (t *compactionTrigger) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("compaction trigger exist")
			return
		case signal := <-t.signals:
			switch {
			case signal.isForce:
				t.handleForceSignal(signal)
			case signal.isGlobal:
				t.handleGlobalSignal(signal)
			default:
				t.handleSignal(signal)
			}
		}
	}
}

// triggerCompaction trigger a compaction if any compaction condition satisfy.
func (t *compactionTrigger) triggerCompaction(timetravel *timetravel) {
	signal := &compactionSignal{
		isForce:    false,
		isGlobal:   true,
		timetravel: timetravel,
	}
	t.signals <- signal
}

// triggerSingleCompaction triger a compaction bundled with collection-partiiton-channel-segment
func (t *compactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, timetravel *timetravel) {
	signal := &compactionSignal{
		isForce:      false,
		isGlobal:     false,
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		channel:      channel,
		timetravel:   timetravel,
	}
	t.signals <- signal
}

// forceTriggerCompaction force to start a compaction
func (t *compactionTrigger) forceTriggerCompaction(collectionID int64, timetravel *timetravel) {
	signal := &compactionSignal{
		isForce:      true,
		isGlobal:     false,
		collectionID: collectionID,
		timetravel:   timetravel,
	}
	t.signals <- signal
}

func (t *compactionTrigger) handleForceSignal(signal *compactionSignal) {
	t1 := time.Now()
	// single compaction is forced to merge all insert and delta binlogs
	if t.compactionHandler.isFull() {
		return
	}

	if err := t.globalMergeCompaction(signal.timetravel, signal.collectionID); err != nil {
		log.Warn("failed to do force compaction", zap.Error(err), zap.Int64("collectionID", signal.collectionID))
		return
	}
	log.Info("handle force signal cost", zap.Int64("milliseconds", int64(time.Since(t1).Milliseconds())),
		zap.Int64("collectionID", signal.collectionID))
}

func (t *compactionTrigger) handleGlobalSignal(signal *compactionSignal) {
	t1 := time.Now()
	// 1. find all segments needed to be
	if t.compactionHandler.isFull() {
		return
	}

	// 2. find channel&partition level segments needed to be merged
	if t.compactionHandler.isFull() {
		return
	}

	// FIXME: collections only contains collections cached in datacoord, actually we should get all collections
	collections := t.meta.GetCollectionsID()
	if len(collections) == 0 {
		return
	}

	if err := t.globalMergeCompaction(signal.timetravel, collections...); err != nil {
		log.Warn("failed to do global compaction", zap.Error(err))
		return
	}

	log.Info("handle glonbal compaction cost", zap.Int64("millliseconds", time.Since(t1).Milliseconds()))
}

func (t *compactionTrigger) handleSignal(signal *compactionSignal) {
	t1 := time.Now()
	// 1. check whether segment's binlogs should be compacted or not
	if t.compactionHandler.isFull() {
		return
	}

	segment := t.meta.GetSegment(signal.segmentID)
	singleCompactionPlan, err := t.singleCompaction(segment, signal.timetravel)
	if err != nil {
		log.Warn("failed to do single compaction", zap.Int64("segmentID", segment.ID), zap.Error(err))
	} else {
		log.Info("time cost of generating single compaction plan", zap.Int64("milllis", time.Since(t1).Milliseconds()),
			zap.Int64("planID", singleCompactionPlan.GetPlanID()))
	}

	// 2. check whether segments of partition&channel level should be compacted or not
	if t.compactionHandler.isFull() {
		return
	}

	channel := segment.GetInsertChannel()
	partitionID := segment.GetPartitionID()

	segments := t.getCandidateSegments(channel, partitionID)

	plan, err := t.mergeCompaction(segments, signal.timetravel)
	if err != nil {
		log.Warn("failed to do merge compaction", zap.Error(err))
	}

	log.Info("time cost of generating merge compaction", zap.Int64("planID", plan.PlanID), zap.Any("time cost", time.Since(t1).Milliseconds()),
		zap.String("channel", channel), zap.Int64("partitionID", partitionID))
}

func (t *compactionTrigger) globalMergeCompaction(timetravel *timetravel, collections ...UniqueID) error {
	m := t.getGlobalCandidateSegments(collections...) // m is map[channel]map[partition][]segment

	for _, v := range m {
		for _, segments := range v {
			if t.compactionHandler.isFull() {
				return nil
			}
			_, err := t.mergeCompaction(segments, timetravel)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *compactionTrigger) mergeCompaction(segments []*SegmentInfo, timetravel *timetravel) (*datapb.CompactionPlan, error) {
	if !t.shouldDoMergeCompaction(segments) {
		return nil, nil
	}

	plan := t.mergeCompactionPolicy.generatePlan(segments, timetravel)
	if plan == nil {
		return nil, nil
	}

	if err := t.fillOriginPlan(plan); err != nil {
		return nil, err
	}

	if err := t.compactionHandler.execCompactionPlan(plan); err != nil {
		return nil, err
	}
	return plan, nil
}

func (t *compactionTrigger) getGlobalCandidateSegments(collections ...UniqueID) map[string]map[UniqueID][]*SegmentInfo {
	segments := make([]*SegmentInfo, 0)
	for _, c := range collections {
		segments = append(segments, t.meta.GetSegmentsOfCollection(c)...)
	}

	if len(segments) == 0 {
		return nil
	}

	m := make(map[string]map[UniqueID][]*SegmentInfo)
	for _, s := range segments {
		insertChannel := s.GetInsertChannel()
		partition := s.GetPartitionID()
		if _, ok := m[insertChannel]; !ok {
			m[insertChannel] = make(map[UniqueID][]*SegmentInfo)
		}
		m[insertChannel][partition] = append(m[insertChannel][partition], s)
	}

	return m
}

func (t *compactionTrigger) getCandidateSegments(channel string, partitionID UniqueID) []*SegmentInfo {
	segments := t.meta.GetSegmentsByChannel(channel)
	res := make([]*SegmentInfo, 0)
	for _, s := range segments {
		if s.GetState() != commonpb.SegmentState_Flushed || s.GetInsertChannel() != channel ||
			s.GetPartitionID() != partitionID || s.isCompacting {
			continue
		}
		res = append(res, s)
	}
	return res
}

func (t *compactionTrigger) shouldDoMergeCompaction(segments []*SegmentInfo) bool {
	littleSegmentNum := 0
	for _, s := range segments {
		if s.GetNumOfRows() < s.GetMaxRowNum()/2 {
			littleSegmentNum++
		}
	}
	return littleSegmentNum > maxLittleSegmentNum
}

func (t *compactionTrigger) fillOriginPlan(plan *datapb.CompactionPlan) error {
	// TODO context
	id, err := t.allocator.allocID(context.Background())
	if err != nil {
		return err
	}
	plan.PlanID = id
	plan.TimeoutInSeconds = maxCompactionTimeoutInSeconds
	return nil
}

func (t *compactionTrigger) shouldDoSingleCompaction(segment *SegmentInfo, timetravel *timetravel) bool {
	// single compaction only merge insert and delta log beyond the timetravel
	// segment's insert binlogs dont have time range info, so we wait until the segment's last expire time is less than timetravel
	// to ensure that all insert logs is beyond the timetravel.
	// TODO: add meta in insert binlog
	if segment.LastExpireTime >= timetravel.time {
		return false
	}

	deltaLogs := make([]*datapb.DeltaLogInfo, 0)
	totalDeletedRows := 0
	for _, l := range segment.GetDeltalogs() {
		if l.TimestampTo < timetravel.time {
			deltaLogs = append(deltaLogs, l)
			totalDeletedRows += int(l.GetRecordEntries())
		}
	}

	// TODO: other policy
	return float32(totalDeletedRows)/float32(segment.NumOfRows) >= singleCompactionRatioThreshold
}

func (t *compactionTrigger) globalSingleCompaction(segments []*SegmentInfo, timetravel *timetravel) error {
	for _, segment := range segments {
		if t.compactionHandler.isFull() {
			return nil
		}
		_, err := t.singleCompaction(segment, timetravel)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *compactionTrigger) singleCompaction(segment *SegmentInfo, timetravel *timetravel) (*datapb.CompactionPlan, error) {
	if segment == nil {
		return nil, nil
	}

	if !t.shouldDoSingleCompaction(segment, timetravel) {
		return nil, nil
	}

	plan := t.singleCompactionPolicy.generatePlan(segment, timetravel)
	if plan == nil {
		return nil, nil
	}

	if err := t.fillOriginPlan(plan); err != nil {
		return nil, err
	}
	return plan, t.compactionHandler.execCompactionPlan(plan)
}
