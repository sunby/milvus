package vchannel

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func fanoutTestVChannel(collectionID int64) string {
	return funcutil.GetVirtualChannel("p1", collectionID, 0)
}

// newFanoutTestManager creates a manager with collections 1..vchannels.
func newFanoutTestManager(tb testing.TB, vchannels int) *PChannelRecoveryManager {
	tb.Helper()
	metas := make(map[string]*streamingpb.VChannelMeta, vchannels)
	for i := int64(1); i <= int64(vchannels); i++ {
		vchannel := fanoutTestVChannel(i)
		metas[vchannel] = &streamingpb.VChannelMeta{
			Vchannel:                      vchannel,
			State:                         streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:            1,
			TransformMaterializedTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: i,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: i, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
					Schema:             &schemapb.CollectionSchema{Name: vchannel},
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: 1,
				}},
			},
		}
	}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1", VChannelMetas: metas})
	require.NoError(tb, err)
	tb.Cleanup(manager.Close)
	return manager
}

func newPChannelTimeTick(timeTick uint64) message.ImmutableMessage {
	return message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		MustBuildMutable().
		WithTimeTick(timeTick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timeTick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timeTick)))
}

// observeTracked observes msg as recovery storage does and reports whether
// every handle of it was released.
func observeTracked(manager *PChannelRecoveryManager, msg message.ImmutableMessage) bool {
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	dispatch := owner.Clone()
	manager.ObserveMessage(context.Background(), dispatch)
	dispatch.Release()
	owner.Release()
	return tracker.CompletedPoint().TimeTick == msg.TimeTick()
}

func TestTimeTickAdvancesIdleModulesWithoutDirtyingThem(t *testing.T) {
	barrier := message.NewMVCCBarrierMessageBuilderV2().
		WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{fanoutTestVChannel(1)}}).
		WithBody(&message.MVCCBarrierMessageBody{}).WithAllVChannel().MustBuildMutable().
		WithTimeTick(200).WithLastConfirmed(walimplstest.NewTestMessageID(200)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(200))
	for _, msg := range []message.ImmutableMessage{newPChannelTimeTick(100), barrier} {
		manager := newFanoutTestManager(t, 3)
		require.True(t, observeTracked(manager, msg), "no module retains a %s", msg.MessageType())
		for collectionID := int64(1); collectionID <= 3; collectionID++ {
			require.Equal(t, msg.TimeTick(), manager.Module(fanoutTestVChannel(collectionID)).queryObservedTimeTick)
		}
		require.Empty(t, manager.takeDirtyModules(), "a %s changes no persisted state of an idle module", msg.MessageType())
	}
}

func TestTimeTickLeavesClosedModulesUnchanged(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	require.True(t, observeTracked(manager, newPChannelTimeTick(100)))
	require.Zero(t, manager.Module("v1").queryObservedTimeTick)
	require.Empty(t, manager.takeDirtyModules())
}

func TestTimeTickTakesTheFullPathOnModulesWithPendingDrops(t *testing.T) {
	manager := newFanoutTestManager(t, 2)
	busy := manager.Module(fanoutTestVChannel(1))
	// A Drop that cannot complete yet: nothing is materialized through it.
	drop := message.NewOwnedImmutableMessage(newPChannelTimeTick(1000), nil)
	busy.mu.Lock()
	busy.pendingDrops = append(busy.pendingDrops, drop.Clone())
	busy.mu.Unlock()

	require.True(t, observeTracked(manager, newPChannelTimeTick(100)))
	require.EqualValues(t, 100, busy.queryObservedTimeTick)
	require.EqualValues(t, 100, manager.Module(fanoutTestVChannel(2)).queryObservedTimeTick)
	dirty := manager.takeDirtyModules()
	require.Len(t, dirty, 1, "only ObserveMessage marks a module dirty")
	require.Same(t, busy, dirty[fanoutTestVChannel(1)])

	busy.mu.Lock()
	pending := busy.pendingDrops
	busy.pendingDrops = nil
	busy.mu.Unlock()
	releaseDropHandles(pending)
	drop.Release()
}

func TestTimeTickReachesModulesAddedAndSkipsModulesRemoved(t *testing.T) {
	manager := newFanoutTestManager(t, 2)
	require.Len(t, manager.modules.List(), 2)

	schema, err := proto.Marshal(&schemapb.CollectionSchema{Name: "added"})
	require.NoError(t, err)
	added := fanoutTestVChannel(3)
	require.True(t, observeTracked(manager, message.NewCreateCollectionMessageBuilderV1().WithVChannel(added).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 3}).
		WithBody(&msgpb.CreateCollectionRequest{Schema: schema}).MustBuildMutable().
		WithTimeTick(50).WithLastConfirmed(walimplstest.NewTestMessageID(50)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(50))))
	require.NotNil(t, manager.Module(added))
	require.True(t, observeTracked(manager, newPChannelTimeTick(100)))
	require.EqualValues(t, 100, manager.Module(added).queryObservedTimeTick)

	removed := manager.Module(fanoutTestVChannel(1))
	manager.removeModule(removed)
	require.NotContains(t, manager.modules.List(), removed)
	require.Len(t, manager.modules.List(), 2)
}

// BenchmarkObserveTimeTickFanout measures how one pchannel-level TimeTick is
// dispatched to every vchannel module of a pchannel.
func BenchmarkObserveTimeTickFanout(b *testing.B) {
	for _, vchannels := range []int{1000, 30000} {
		b.Run(fmt.Sprintf("vchannels=%d", vchannels), func(b *testing.B) {
			manager := newFanoutTestManager(b, vchannels)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				observeTracked(manager, newPChannelTimeTick(uint64(100+i)))
			}
		})
	}
}
