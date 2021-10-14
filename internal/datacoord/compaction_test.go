package datacoord

import (
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

// TODO not completed

func Test_compactionPlanHandler_execCompactionPlan(t *testing.T) {
	type fields struct {
		plans    map[int64]*compactionTask
		sessions *SessionManager
	}
	type args struct {
		plan *datapb.CompactionPlan
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		err     error
	}{
		{
			"test exec compaction",
			fields{
				plans: map[int64]*compactionTask{},
				sessions: &SessionManager{
					sessions: struct {
						sync.RWMutex
						data map[int64]*Session
					}{
						data: map[int64]*Session{1: {info: &NodeInfo{1, "localhost:9999"}}},
					},
				},
			},
			args{
				plan: &datapb.CompactionPlan{PlanID: 1, Type: datapb.CompactionType_MergeCompaction},
			},
			false,
			nil,
		},
		{
			"test exec compaction with no session",
			fields{
				plans: map[int64]*compactionTask{},
				sessions: &SessionManager{
					sessions: struct {
						sync.RWMutex
						data map[int64]*Session
					}{
						data: map[int64]*Session{},
					},
				},
			},
			args{
				plan: &datapb.CompactionPlan{PlanID: 1, Type: datapb.CompactionType_MergeCompaction},
			},
			true,
			errNotEnoughDataNode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:    tt.fields.plans,
				sessions: tt.fields.sessions,
			}
			err := c.execCompactionPlan(tt.args.plan)
			assert.Equal(t, tt.err, err)
			if err == nil {
				task := c.getCompaction(tt.args.plan.PlanID)
				assert.Equal(t, tt.args.plan, task.plan)
			}
		})
	}
}

func Test_compactionPlanHandler_completeCompaction(t *testing.T) {
	type fields struct {
		plans    map[int64]*compactionTask
		sessions *SessionManager
	}
	type args struct {
		result *datapb.CompactionResult
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test complete non existed compaction task",
			fields{
				plans: map[int64]*compactionTask{1: {}},
			},
			args{
				result: &datapb.CompactionResult{PlanID: 2},
			},
			true,
		},
		{
			"test complete completed task",
			fields{
				plans: map[int64]*compactionTask{1: {state: completed}},
			},
			args{
				result: &datapb.CompactionResult{PlanID: 1},
			},
			true,
		},
		{
			"test complete executing task",
			fields{
				plans: map[int64]*compactionTask{1: {state: executing}},
			},
			args{
				result: &datapb.CompactionResult{PlanID: 1},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:    tt.fields.plans,
				sessions: tt.fields.sessions,
			}
			err := c.completeCompaction(tt.args.result)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func Test_compactionPlanHandler_getCompaction(t *testing.T) {
	type fields struct {
		plans    map[int64]*compactionTask
		sessions *SessionManager
	}
	type args struct {
		planID int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *compactionTask
	}{
		{
			"test get non existed task",
			fields{plans: map[int64]*compactionTask{}},
			args{planID: 1},
			nil,
		},
		{
			"test get existed task",
			fields{
				plans: map[int64]*compactionTask{1: {
					state: executing,
				}},
			},
			args{planID: 1},
			&compactionTask{
				state: executing,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:    tt.fields.plans,
				sessions: tt.fields.sessions,
			}
			got := c.getCompaction(tt.args.planID)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func Test_compactionPlanHandler_expireCompaction(t *testing.T) {
	type fields struct {
		plans    map[int64]*compactionTask
		sessions *SessionManager
	}
	type args struct {
		ts Timestamp
	}

	ts := time.Now()
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		expired   []int64
		unexpired []int64
	}{
		{
			"test expire compaction task",
			fields{
				plans: map[int64]*compactionTask{
					1: {
						state: executing,
						plan: &datapb.CompactionPlan{
							PlanID:           1,
							StartTime:        tsoutil.ComposeTS(ts.UnixNano()/int64(time.Millisecond), 0),
							TimeoutInSeconds: 10,
						},
					},
					2: {
						state: executing,
						plan: &datapb.CompactionPlan{
							PlanID:           2,
							StartTime:        tsoutil.ComposeTS(ts.UnixNano()/int64(time.Millisecond), 0),
							TimeoutInSeconds: 1,
						},
					},
				},
			},
			args{ts: tsoutil.ComposeTS(ts.Add(5*time.Second).UnixNano()/int64(time.Millisecond), 0)},
			false,
			[]int64{2},
			[]int64{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:    tt.fields.plans,
				sessions: tt.fields.sessions,
			}

			err := c.expireCompaction(tt.args.ts)
			assert.Equal(t, tt.wantErr, err != nil)

			for _, id := range tt.expired {
				task := c.getCompaction(id)
				assert.Equal(t, timeout, task.state)
			}

			for _, id := range tt.unexpired {
				task := c.getCompaction(id)
				assert.NotEqual(t, timeout, task.state)
			}
		})
	}
}
