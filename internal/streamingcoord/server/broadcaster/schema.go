package broadcaster

import (
	"maps"
	"sync"

	"github.com/google/btree"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SchemaTarget is an owned snapshot of durable ALTER intent. It is available
// only after the ordered ACK callback may prepare resources. RootCoord's
// published schema takes over when that callback completes and is tombstoned.
type SchemaTarget struct {
	CollectionID int64
	BroadcastID  uint64
	Schema       *schemapb.CollectionSchema
	TimeTicks    map[string]uint64
}

type pendingSchemaTarget struct {
	target          *SchemaTarget
	controlTimeTick uint64
	eligible        bool
}

// schemaTargetCollectionID reads only the immutable header. In particular,
// recovering completed tasks must not decode their schema bodies.
func schemaTargetCollectionID(msg message.BroadcastMutableMessage) int64 {
	if msg.MessageTypeWithVersion() != message.MessageTypeAlterCollectionV2 {
		return 0
	}
	alter, err := message.AsMutableAlterCollectionMessageV2(msg)
	if err != nil || !messageutil.IsSchemaChange(alter.Header()) {
		return 0
	}
	return alter.Header().GetCollectionId()
}

// schemaTaskIndex contains immutable snapshots of durable, unfinished schema
// tasks. Readers never acquire manager/task locks or inspect unrelated tasks.
// Writers hold the task lock before taking the index lock, never the reverse.
type schemaTaskIndex struct {
	mu          sync.RWMutex
	collections map[int64]*collectionSchemaTasks
}

type collectionSchemaTasks struct {
	pending *btree.BTreeG[*pendingSchemaTarget]
	head    *pendingSchemaTarget
}

func newSchemaTaskIndex() *schemaTaskIndex {
	return &schemaTaskIndex{collections: make(map[int64]*collectionSchemaTasks)}
}

// update runs under the task lock after a successful save, or before recovery
// publishes the manager. Unpersisted mutations cannot replace the last snapshot.
func (i *schemaTaskIndex) update(task *broadcastTask) {
	if task.schemaCollectionID == 0 {
		return
	}
	snapshot := task.pendingSchemaTarget()
	i.replace(task.schemaCollectionID, task.indexedSchemaTarget, snapshot)
	task.indexedSchemaTarget = snapshot
}

func (i *schemaTaskIndex) replace(collectionID int64, previous, next *pendingSchemaTarget) {
	i.mu.Lock()
	defer i.mu.Unlock()
	tasks := i.collections[collectionID]
	if tasks != nil && previous != nil {
		// ACK persistence can change the ordering key. Delete the old immutable
		// snapshot before inserting its replacement under the new key.
		tasks.pending.Delete(previous)
	}
	if next != nil {
		if tasks == nil {
			tasks = &collectionSchemaTasks{pending: btree.NewG(2, func(a, b *pendingSchemaTarget) bool {
				if a.controlTimeTick != b.controlTimeTick {
					return a.controlTimeTick < b.controlTimeTick
				}
				return a.target.BroadcastID < b.target.BroadcastID
			})}
			i.collections[collectionID] = tasks
		}
		tasks.pending.ReplaceOrInsert(next)
	}
	if tasks == nil {
		return
	}
	if tasks.pending.Len() == 0 {
		delete(i.collections, collectionID)
	} else {
		tasks.head, _ = tasks.pending.Min()
	}
}

// pendingSchemaTarget is called with the task lock held, or before publication.
func (b *broadcastTask) pendingSchemaTarget() *pendingSchemaTarget {
	if b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE || b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		b.schemaForTarget = nil
		return nil
	}
	if b.schemaForTarget == nil {
		msg := message.MustAsMutableAlterCollectionMessageV2(b.msg)
		schema := msg.MustBody().GetUpdates().GetSchema()
		if schema == nil {
			return nil
		}
		// The ALTER body is immutable. Cache one owned copy for all persisted
		// ACK snapshots; readers receive their own copy of the selected schema.
		b.schemaForTarget = proto.Clone(schema).(*schemapb.CollectionSchema)
	}
	snapshot := &pendingSchemaTarget{
		target:   &SchemaTarget{CollectionID: b.schemaCollectionID, BroadcastID: b.header().BroadcastID, Schema: b.schemaForTarget, TimeTicks: make(map[string]uint64)},
		eligible: b.schemaTargetEligible,
	}
	for i, channel := range b.header().VChannels {
		tick := b.task.AckedCheckpoints[i].GetTimeTick()
		if funcutil.IsControlChannel(channel) {
			snapshot.controlTimeTick = tick
		} else {
			snapshot.target.TimeTicks[channel] = tick
		}
	}
	return snapshot
}

// markSchemaTargetEligible preserves ordered callback admission without
// republishing task mutations that have not been saved successfully.
func (b *broadcastTask) markSchemaTargetEligible() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.schemaTargetEligible = true
	if previous := b.indexedSchemaTarget; previous != nil {
		next := *previous
		next.eligible = true
		b.schemaTasks.replace(b.schemaCollectionID, previous, &next)
		b.indexedSchemaTarget = &next
	}
}

func (bm *broadcastTaskManager) GetPendingSchemaTarget(collectionID int64) (*SchemaTarget, error) {
	bm.schemaTasks.mu.RLock()
	tasks := bm.schemaTasks.collections[collectionID]
	if tasks == nil {
		bm.schemaTasks.mu.RUnlock()
		return nil, nil
	}
	snapshot := tasks.head
	bm.schemaTasks.mu.RUnlock()
	if !snapshot.eligible {
		return nil, merr.WrapErrServiceNotReadyMsg("schema target for collection %d awaits its ordered ACK callback", collectionID)
	}
	return &SchemaTarget{
		CollectionID: snapshot.target.CollectionID,
		BroadcastID:  snapshot.target.BroadcastID,
		Schema:       proto.Clone(snapshot.target.Schema).(*schemapb.CollectionSchema),
		TimeTicks:    maps.Clone(snapshot.target.TimeTicks),
	}, nil
}
