package datacoord

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

type spyCompactionHandler struct {
	spyChan chan struct{}
}

// execCompactionPlan start to execute plan and return immediately
func (h *spyCompactionHandler) execCompactionPlan(plan *datapb.CompactionPlan) error {
	h.spyChan <- struct{}{}
	return nil
}

// completeCompaction record the result of a compaction
func (h *spyCompactionHandler) completeCompaction(result *datapb.CompactionResult) error {
	panic("not implemented") // TODO: Implement
}

// getCompaction return compaction task. If planId does not exist, return nil.
func (h *spyCompactionHandler) getCompaction(planID int64) *compactionTask {
	panic("not implemented") // TODO: Implement
}

// expireCompaction set the compaction state to expired
func (h *spyCompactionHandler) expireCompaction(ts Timestamp) error {
	panic("not implemented") // TODO: Implement
}

// isFull return true if the task pool is full
func (h *spyCompactionHandler) isFull() bool {
	return false
}

func Test_compactionTrigger_triggerCompaction(t *testing.T) {
	type fields struct {
		signals chan *compactionSignal
	}
	type args struct {
		timetravel *timetravel
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"test trigger", fields{signals: make(chan *compactionSignal, 1)}, args{timetravel: &timetravel{100}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				signals: tt.fields.signals,
			}
			tr.triggerCompaction(tt.args.timetravel)
			signal := <-tt.fields.signals
			assert.Equal(t, &compactionSignal{isForce: false, isGlobal: true, timetravel: tt.args.timetravel}, signal)
		})
	}
}

func Test_compactionTrigger_triggerSingleCompaction(t *testing.T) {
	type fields struct {
		signals chan *compactionSignal
	}
	type args struct {
		collectionID int64
		partitionID  int64
		segmentID    int64
		channel      string
		timetravel   *timetravel
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"test single trigger", fields{signals: make(chan *compactionSignal, 1)}, args{collectionID: 1, partitionID: 1, segmentID: 1, channel: "chan1", timetravel: &timetravel{100}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				signals: tt.fields.signals,
			}
			tr.triggerSingleCompaction(tt.args.collectionID, tt.args.partitionID, tt.args.segmentID, tt.args.channel, tt.args.timetravel)
			signal := <-tt.fields.signals
			assert.Equal(t, &compactionSignal{
				isForce:      false,
				isGlobal:     false,
				collectionID: tt.args.collectionID,
				partitionID:  tt.args.partitionID,
				segmentID:    tt.args.segmentID,
				channel:      tt.args.channel,
				timetravel:   tt.args.timetravel,
			}, signal)
		})
	}
}

func Test_compactionTrigger_forceTriggerCompaction(t *testing.T) {
	type fields struct {
		signals chan *compactionSignal
	}
	type args struct {
		collectionID int64
		timetravel   *timetravel
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"test force trigger", fields{signals: make(chan *compactionSignal, 1)}, args{collectionID: 1, timetravel: &timetravel{100}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				signals: tt.fields.signals,
			}
			tr.forceTriggerCompaction(tt.args.collectionID, tt.args.timetravel)
			signal := <-tt.fields.signals
			assert.Equal(t, &compactionSignal{
				isForce:      true,
				isGlobal:     false,
				collectionID: tt.args.collectionID,
				timetravel:   tt.args.timetravel,
			}, signal)
		})
	}
}
