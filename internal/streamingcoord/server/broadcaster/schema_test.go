package broadcaster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func schemaTestMessage(collection int64, id uint64) message.BroadcastMutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{
		CollectionId: collection, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
	}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Version: 2, Name: "original"}}}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(collection, fmt.Sprint(id))).
		WithBroadcast([]string{"p" + funcutil.ControlChannelSuffix, "v1", "v2"}).MustBuildBroadcast().
		OverwriteBroadcastHeader(id, message.NewExclusiveCollectionNameResourceKey("db", fmt.Sprint(collection)))
}

func schemaTestTask(collection int64, id, tick uint64, version int32) *broadcastTask {
	msg := schemaTestMessage(collection, id)
	alter := message.MustAsMutableAlterCollectionMessageV2(msg)
	body := alter.MustBody()
	body.Updates.Schema.Version = version
	alter.OverwriteBody(body)
	pb := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{1, 1, 1})
	for i, channel := range msg.BroadcastHeader().VChannels {
		cp := pb.AckedCheckpoints[i]
		switch channel {
		case "v1":
			cp.TimeTick = tick + 1
		case "v2":
			cp.TimeTick = tick + 2
		default:
			cp.TimeTick = tick
		}
	}
	return newBroadcastTaskFromProto(pb, newBroadcasterMetrics(), nil)
}

func newSchemaTestManager(tasks ...*broadcastTask) *broadcastTaskManager {
	bm := &broadcastTaskManager{
		mu: &sync.Mutex{}, tasks: make(map[uint64]*broadcastTask), schemaTasks: newSchemaTaskIndex(),
		metrics: newBroadcasterMetrics(), idempotencyIndex: newIdempotencyIndex(), resourceKeyLocker: newResourceKeyLocker(),
	}
	bm.SetLogger(mlog.With())
	for _, task := range tasks {
		task.SetLogger(mlog.With())
		task.schemaTasks = bm.schemaTasks
		bm.schemaTasks.update(task)
		bm.tasks[task.Header().BroadcastID] = task
		bm.idempotencyIndex.Add(task.IdempotencyScope(), task.Header().BroadcastID)
	}
	return bm
}

func schemaTestAck(msg message.BroadcastMutableMessage, channel string, tick uint64) message.ImmutableMessage {
	for _, part := range msg.SplitIntoMutableMessage() {
		if part.VChannel() == channel {
			return part.WithTimeTick(tick).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
		}
	}
	panic("missing test channel")
}

func requireSchemaTarget(t *testing.T, bm *broadcastTaskManager, collectionID int64, id uint64) *SchemaTarget {
	t.Helper()
	target, err := bm.GetPendingSchemaTarget(collectionID)
	require.NoError(t, err)
	if id == 0 {
		require.Nil(t, target)
	} else {
		require.NotNil(t, target)
		require.Equal(t, id, target.BroadcastID)
	}
	return target
}

func TestSchemaTargetFollowsOrderedCallback(t *testing.T) {
	paramtable.Init()
	// WAL order is authoritative even if recovered IDs have a different order.
	first, second := schemaTestTask(100, 200, 10, 1), schemaTestTask(100, 100, 20, 2)
	bm := newSchemaTestManager(first, second)
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "recovery alone cannot authorize a target")
	second.markSchemaTargetEligible()
	_, err = bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "must not skip a predecessor")
	first.markSchemaTargetEligible()
	got, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 200, got.BroadcastID)
	require.EqualValues(t, 1, got.Schema.Version)
	require.Equal(t, map[string]uint64{"v1": 11, "v2": 12}, got.TimeTicks)
	got.Schema.Name = "changed"
	got.TimeTicks["v1"] = 900
	again, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.Equal(t, "original", again.Schema.Name)
	require.EqualValues(t, 11, again.TimeTicks["v1"])
	first.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	bm.schemaTasks.update(first)
	got, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 2, got.Schema.Version)
	second.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	bm.schemaTasks.update(second)
	got, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.Nil(t, got, "published metadata takes over after completion")
	got, err = bm.GetPendingSchemaTarget(999)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestSchemaTargetIndexReordersPersistedCheckpoints(t *testing.T) {
	paramtable.Init()
	first, second := schemaTestTask(100, 200, 0, 1), schemaTestTask(100, 100, 20, 2)
	bm := newSchemaTestManager(first, second)
	second.markSchemaTargetEligible()
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "an unknown CChannel tick still blocks its successor")
	controlIdx := findIdxOfVChannel("p"+funcutil.ControlChannelSuffix, first.header().VChannels)
	first.mu.Lock()
	first.task.AckedCheckpoints[controlIdx] = &streamingpb.AckedCheckpoint{TimeTick: 30}
	bm.schemaTasks.update(first)
	first.mu.Unlock()
	require.EqualValues(t, 30, first.indexedSchemaTarget.controlTimeTick)
	require.EqualValues(t, 20, second.indexedSchemaTarget.controlTimeTick)
	first.markSchemaTargetEligible()
	requireSchemaTarget(t, bm, 100, 100)
	require.Equal(t, 2, bm.schemaTasks.collections[100].pending.Len(), "updating an ordering key must replace, not duplicate, its task")

	first.mu.Lock()
	first.task.AckedCheckpoints[controlIdx].TimeTick = 20
	bm.schemaTasks.update(first)
	first.mu.Unlock()
	requireSchemaTarget(t, bm, 100, 100) // Equal ticks use BroadcastID as the tie breaker.
	second.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	bm.schemaTasks.update(second)
	requireSchemaTarget(t, bm, 100, 200)
}

func TestSchemaTargetPersistenceBoundary(t *testing.T) {
	paramtable.Init()
	var persisted *streamingpb.BroadcastTask
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ uint64, task *streamingpb.BroadcastTask) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			persisted = proto.Clone(task).(*streamingpb.BroadcastTask)
			return nil
		})
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	bm := newSchemaTestManager()
	msg := schemaTestMessage(100, 1)
	guards := bm.resourceKeyLocker.Lock(message.NewSharedClusterResourceKey())
	t.Cleanup(guards.Unlock)
	task, created := bm.getOrAddBroadcastTask(context.Background(), msg, 1, guards)
	require.True(t, created)
	requireSchemaTarget(t, bm, 100, 0)
	require.Nil(t, task.schemaForTarget, "registration does not decode an unpersisted schema")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, task.InitializeRecovery(canceled), context.Canceled)
	require.True(t, task.dirty)
	requireSchemaTarget(t, bm, 100, 0)
	require.NoError(t, task.InitializeRecovery(context.Background()))
	require.False(t, task.dirty)
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "durability alone does not authorize preparation")
	task.markSchemaTargetEligible()
	before := requireSchemaTarget(t, bm, 100, 1)
	require.Equal(t, map[string]uint64{"v1": 0, "v2": 0}, before.TimeTicks)

	ack := schemaTestAck(msg, "v1", 10)
	require.ErrorIs(t, task.Ack(canceled, ack), context.Canceled)
	require.True(t, task.dirty)
	// Callback admission must only publish the last durable snapshot, even if a
	// later ACK has failed persistence while the in-memory task already has it.
	task.markSchemaTargetEligible()
	require.EqualValues(t, 0, requireSchemaTarget(t, bm, 100, 1).TimeTicks["v1"])
	require.NoError(t, task.Ack(context.Background(), ack))
	require.EqualValues(t, 10, requireSchemaTarget(t, bm, 100, 1).TimeTicks["v1"])
	require.EqualValues(t, 0, before.TimeTicks["v1"], "old snapshots stay immutable")

	// Recovery derives the index exclusively from durable tasks and resets the
	// process-local callback authorization, including on a secondary.
	persisted.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED
	catalog.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{persisted}, nil)
	recovered, err := RecoverBroadcaster(context.Background())
	require.NoError(t, err)
	next := recovered.(*broadcastTaskManager)
	t.Cleanup(next.Close)
	_, err = next.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	next.tasks[1].markSchemaTargetEligible()
	require.EqualValues(t, 10, requireSchemaTarget(t, next, 100, 1).TimeTicks["v1"])

	require.ErrorIs(t, task.MarkAckCallbackDone(canceled), context.Canceled)
	require.True(t, task.dirty)
	requireSchemaTarget(t, bm, 100, 1)
	require.NoError(t, task.MarkAckCallbackDone(context.Background()))
	requireSchemaTarget(t, bm, 100, 0)
	require.Empty(t, bm.schemaTasks.collections)
	require.Nil(t, task.schemaForTarget)
	require.Contains(t, bm.tasks, uint64(1), "schema retirement must not remove the tombstone")
	duplicate, created := bm.getOrAddBroadcastTask(context.Background(), msg, 2, nil)
	require.False(t, created, "tombstones must still provide idempotent lookup")
	require.Same(t, task, duplicate)
}

func TestSchemaTargetIndexExclusions(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name  string
		path  string
		state streamingpb.BroadcastTaskState
	}{
		{"properties", message.FieldMaskCollectionProperties, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING},
		{"tombstone", message.FieldMaskCollectionSchema, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE},
		{"done", message.FieldMaskCollectionSchema, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := schemaTestMessage(100, 1)
			alter := message.MustAsMutableAlterCollectionMessageV2(msg)
			header := alter.Header()
			header.UpdateMask.Paths = []string{tc.path}
			alter.OverwriteHeader(header)
			pb := createNewWaitAckBroadcastTaskFromMessage(msg, tc.state, []byte{1, 1, 1})
			pb.Message.Payload = []byte{0xff} // Excluded tasks must not decode their bodies.
			bm := newSchemaTestManager(newBroadcastTaskFromProto(pb, newBroadcasterMetrics(), nil))
			require.Empty(t, bm.schemaTasks.collections)
			requireSchemaTarget(t, bm, 100, 0)
		})
	}
	t.Run("missing schema", func(t *testing.T) {
		msg := schemaTestMessage(100, 1)
		alter := message.MustAsMutableAlterCollectionMessageV2(msg)
		alter.OverwriteBody(&message.AlterCollectionMessageBody{})
		pb := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{1, 1, 1})
		bm := newSchemaTestManager(newBroadcastTaskFromProto(pb, newBroadcasterMetrics(), nil))
		require.Empty(t, bm.schemaTasks.collections)
		requireSchemaTarget(t, bm, 100, 0)
	})
}

func TestSchemaTargetReplicatedTaskLifecycle(t *testing.T) {
	paramtable.Init()
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	bm := newSchemaTestManager()
	msg := schemaTestMessage(100, 1)
	var ack message.ImmutableMessage
	for _, part := range msg.SplitIntoMutableMessage() {
		if part.VChannel() == "v1" {
			ack = part.WithReplicateHeader(&message.ReplicateHeader{
				ClusterID: "source", MessageID: walimplstest.NewTestMessageID(2), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 10,
			}).WithTimeTick(20).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
		}
	}
	require.NotNil(t, ack)
	task, found := bm.getOrCreateBroadcastTask(ack)
	require.True(t, found)
	require.Same(t, bm.schemaTasks, task.schemaTasks)
	require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, task.State())
	requireSchemaTarget(t, bm, 100, 0)
	require.NoError(t, task.Ack(context.Background(), ack))
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	task.markSchemaTargetEligible()
	before := requireSchemaTarget(t, bm, 100, 1)
	require.EqualValues(t, 20, before.TimeTicks["v1"])
	require.NoError(t, task.Ack(context.Background(), ack))
	require.Equal(t, 1, bm.schemaTasks.collections[100].pending.Len(), "duplicate ACK cannot duplicate an index entry")
	require.NoError(t, task.MarkAckCallbackDone(context.Background()))
	requireSchemaTarget(t, bm, 100, 0)
	require.Contains(t, bm.tasks, uint64(1))
}

func TestSchemaTargetLookupDuringPersistence(t *testing.T) {
	paramtable.Init()
	entered, release := make(chan struct{}), make(chan struct{})
	releaseSave := sync.OnceFunc(func() { close(release) })
	defer releaseSave()
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ uint64, _ *streamingpb.BroadcastTask) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	task := schemaTestTask(100, 1, 10, 2)
	bm := newSchemaTestManager(task)
	task.markSchemaTargetEligible()
	done := make(chan error, 1)
	go func() {
		task.mu.Lock()
		defer task.mu.Unlock()
		idx := findIdxOfVChannel("v1", task.header().VChannels)
		task.task.AckedCheckpoints[idx].TimeTick = 100
		task.dirty = true
		done <- task.saveTaskIfDirty(context.Background(), task.Logger())
	}()
	<-entered
	before := requireSchemaTarget(t, bm, 100, 1)
	require.EqualValues(t, 11, before.TimeTicks["v1"], "blocked metadata I/O must not publish its in-memory mutation")
	releaseSave()
	require.NoError(t, <-done)
	require.EqualValues(t, 100, requireSchemaTarget(t, bm, 100, 1).TimeTicks["v1"])
	require.EqualValues(t, 11, before.TimeTicks["v1"])
}

func TestSchemaTargetLookupIgnoresTombstones(t *testing.T) {
	paramtable.Init()
	tasks := make([]*broadcastTask, 0, 8194)
	for id := uint64(1); id <= 8192; id++ {
		msg := schemaTestMessage(100, id)
		pb := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{1, 1, 1})
		pb.Message.Payload = []byte{0xff}
		tasks = append(tasks, newBroadcastTaskFromProto(pb, newBroadcasterMetrics(), nil))
	}
	target := schemaTestTask(100, 8193, 10, 2)
	unrelated := schemaTestTask(200, 8194, 20, 3)
	tasks = append(tasks, target, unrelated)
	bm := newSchemaTestManager(tasks...)
	target.markSchemaTargetEligible()
	require.Len(t, bm.tasks, 8194)
	require.Len(t, bm.schemaTasks.collections, 2)
	require.Equal(t, 1, bm.schemaTasks.collections[100].pending.Len())
	require.Zero(t, testing.AllocsPerRun(100, func() {
		got, err := bm.GetPendingSchemaTarget(999)
		if got != nil || err != nil {
			panic("unexpected target for an absent collection")
		}
	}))

	bm.mu.Lock()
	for _, task := range tasks {
		task.mu.Lock()
	}
	type lookupResult struct {
		target *SchemaTarget
		err    error
	}
	done := make(chan lookupResult, 1)
	go func() {
		got, err := bm.GetPendingSchemaTarget(100)
		done <- lookupResult{got, err}
	}()
	var result lookupResult
	completed := false
	select {
	case result = <-done:
		completed = true
	case <-time.After(time.Second):
	}
	for _, task := range tasks {
		task.mu.Unlock()
	}
	bm.mu.Unlock()
	if !completed {
		<-done
	}
	require.True(t, completed, "lookup must not acquire any task or manager lock")
	require.NoError(t, result.err)
	require.NotNil(t, result.target)
	require.EqualValues(t, 8193, result.target.BroadcastID)
}

func TestSchemaTargetConcurrentOrderedCallbacks(t *testing.T) {
	configureTombstoneGCTest(t)
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("8192")
	registry.ResetRegistration()
	defer registry.ResetRegistration()
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	first, second := schemaTestTask(100, 200, 10, 1), schemaTestTask(100, 100, 20, 2)
	// Recover a predecessor whose CChannel is ACKed but one business channel
	// is not. The real scheduler must wait before authorizing its schema.
	idx := findIdxOfVChannel("v2", first.header().VChannels)
	first.task.AckedCheckpoints[idx] = nil
	first.task.AckedVchannelBitmap[idx] = 0
	first.allAcked, first.allAckedClosed = make(chan struct{}), false
	bm := newSchemaTestManager(first, second)
	s := newAckCallbackScheduler(mlog.With())
	for _, task := range []*broadcastTask{first, second} {
		task.ackCallbackScheduler = s
	}
	entered := make(chan *SchemaTarget, 2)
	release := make(chan struct{}, 2)
	registry.RegisterAlterCollectionV2AckCallback(func(ctx context.Context, _ message.BroadcastResultAlterCollectionMessageV2) error {
		got, err := bm.GetPendingSchemaTarget(100)
		if err != nil {
			t.Error(err)
			return err
		}
		entered <- got
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	s.Initialize([]*broadcastTask{second, first}, nil, bm)
	defer s.Close()
	require.Eventually(t, func() bool {
		s.rkLockerMu.Lock()
		defer s.rkLockerMu.Unlock()
		guards, ok := s.rkLocker.TryLock(message.NewExclusiveCollectionNameResourceKey("db", "100"))
		if ok {
			guards.Unlock()
		}
		return !ok
	}, 5*time.Second, time.Millisecond)
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	select {
	case <-entered:
		t.Fatal("callback ran before all business channels were ACKed")
	default:
	}
	require.NoError(t, first.Ack(context.Background(), schemaTestAck(first.BroadcastMessage(), "v2", 12)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitTarget := func(id uint64) {
		t.Helper()
		select {
		case got := <-entered:
			require.NotNil(t, got)
			require.Equal(t, id, got.BroadcastID)
		case <-ctx.Done():
			t.Fatal("ordered callback did not enter")
		}
	}
	waitTarget(200)
	var readers sync.WaitGroup
	for n := 0; n < 8; n++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 100; i++ {
				got, err := bm.GetPendingSchemaTarget(100)
				if err != nil {
					assert.ErrorIs(t, err, merr.ErrServiceNotReady)
					continue
				}
				if got != nil {
					assert.Contains(t, []uint64{100, 200}, got.BroadcastID)
					assert.Equal(t, "original", got.Schema.Name)
					got.Schema.Name = "reader-owned"
					got.TimeTicks["v1"] = 900
				}
			}
		}()
	}
	release <- struct{}{}
	waitTarget(100)
	readers.Wait()
	got := requireSchemaTarget(t, bm, 100, 100)
	require.Equal(t, "original", got.Schema.Name)
	require.EqualValues(t, 21, got.TimeTicks["v1"])
	release <- struct{}{}
	require.Eventually(t, func() bool {
		got, err := bm.GetPendingSchemaTarget(100)
		return err == nil && got == nil
	}, 5*time.Second, time.Millisecond)
	for _, task := range []*broadcastTask{first, second} {
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, task.State())
	}
}

func BenchmarkSchemaTargetLookup(b *testing.B) {
	paramtable.Init()
	for _, count := range []int{0, 8192} {
		b.Run(fmt.Sprintf("tombstones=%d", count), func(b *testing.B) {
			tasks := make([]*broadcastTask, 0, count)
			for id := 1; id <= count; id++ {
				msg := schemaTestMessage(100, uint64(id))
				pb := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{1, 1, 1})
				tasks = append(tasks, newBroadcastTaskFromProto(pb, newBroadcasterMetrics(), nil))
			}
			bm := newSchemaTestManager(tasks...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got, err := bm.GetPendingSchemaTarget(100); got != nil || err != nil {
					b.Fatal("unexpected pending target")
				}
			}
		})
	}
}
