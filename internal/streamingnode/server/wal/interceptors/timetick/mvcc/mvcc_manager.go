package mvcc

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// NewMVCCManager creates a new per-vchannel query MVCC manager.
func NewMVCCManager(lastConfirmedTimeTick uint64) *MVCCManager {
	return &MVCCManager{
		lastConfirmedTimeTick: lastConfirmedTimeTick,
		vchannelMVCCs:         make(map[string]vchannelMVCC),
	}
}

// MVCCManager is the manager that manages all the mvcc state of one wal.
// It tracks the persisted query-plan frontiers of each recovered vchannel.
type MVCCManager struct {
	mu                    sync.RWMutex
	lastConfirmedTimeTick uint64 // PChannel-level confirmation frontier.
	vchannelMVCCs         map[string]vchannelMVCC
}

// GetMVCCOfVChannel gets the query MVCC frontiers of the vchannel.
func (cm *MVCCManager) GetMVCCOfVChannel(vchannel string) VChannelMVCC {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if mvcc, ok := cm.vchannelMVCCs[vchannel]; ok {
		latestTimeTick := max(mvcc.GrowingTimetick, mvcc.TransformingTimetick)
		return VChannelMVCC{
			GrowingTimetick:      mvcc.GrowingTimetick,
			TransformingTimetick: mvcc.TransformingTimetick,
			Confirmed:            latestTimeTick <= cm.lastConfirmedTimeTick,
		}
	}
	return VChannelMVCC{}
}

// ApplyRecoveryBarrier initializes or advances the recovered query MVCC baseline
// of one live vchannel.
func (cm *MVCCManager) ApplyRecoveryBarrier(vchannel string, timetick uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	mvcc := cm.vchannelMVCCs[vchannel]
	mvcc.GrowingTimetick = max(mvcc.GrowingTimetick, timetick)
	mvcc.TransformingTimetick = max(mvcc.TransformingTimetick, timetick)
	cm.lastConfirmedTimeTick = max(cm.lastConfirmedTimeTick, timetick)
	cm.vchannelMVCCs[vchannel] = mvcc
}

// UpdateMVCC updates the mvcc state by incoming message.
func (cm *MVCCManager) UpdateMVCC(msg message.MutableMessage) {
	if !msg.IsPersisted() {
		// A unpersisted message is always a time tick message that is used to sync up the system time.
		// No data change should be made by this message so it should be ignored in the mvcc manager.
		return
	}

	tt := msg.TimeTick()
	msgType := msg.MessageType()
	if messageutil.IsTimeTickConfirmBarrier(msgType) {
		cm.sync(tt)
		return
	}

	vchannel := msg.VChannel()
	isTxn := msg.TxnContext() != nil

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// If the message belongs to a transaction, the query MVCC frontiers cannot
	// move forward until the transaction is committed.
	// because of an unconfirmed transaction may be rollback and cannot be seen at read side.
	if isTxn && msgType != message.MessageTypeCommitTxn {
		return
	}
	if vchannel == "" {
		if isPChannelTransformBarrier(msgType) {
			cm.advanceTransformingAllLocked(tt)
		}
		return
	}
	mvcc := cm.vchannelMVCCs[vchannel]
	switch msgType {
	case message.MessageTypeCreateCollection:
		if tt <= max(mvcc.GrowingTimetick, mvcc.TransformingTimetick) {
			return
		}
		mvcc.GrowingTimetick = tt
		mvcc.TransformingTimetick = tt
	case message.MessageTypeInsert:
		if tt <= mvcc.GrowingTimetick {
			return
		}
		mvcc.GrowingTimetick = tt
	case message.MessageTypeDelete:
		if tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
		mvcc.GrowingTimetick = max(mvcc.GrowingTimetick, mvcc.TransformingTimetick)
	case message.MessageTypeCommitTxn:
		if tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
		mvcc.GrowingTimetick = max(mvcc.GrowingTimetick, mvcc.TransformingTimetick)
	case message.MessageTypeFlush,
		message.MessageTypeManualFlush,
		message.MessageTypeDropPartition,
		message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeFlushAll,
		message.MessageTypeAlterWAL:
		if tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
	case message.MessageTypeAlterCollection:
		alter := message.MustAsMutableAlterCollectionMessageV2(msg)
		if !messageutil.IsSchemaChange(alter.Header()) || tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
	default:
		return
	}
	cm.vchannelMVCCs[vchannel] = mvcc
}

// sync advances the pchannel-level confirmation frontier. Whether a vchannel
// MVCC is confirmed is derived when it is read, so no per-vchannel update is
// needed for a time tick confirmation barrier.
func (cm *MVCCManager) sync(tt uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.lastConfirmedTimeTick = max(cm.lastConfirmedTimeTick, tt)
}

func (cm *MVCCManager) advanceTransformingAllLocked(tt uint64) {
	for vchannel, mvcc := range cm.vchannelMVCCs {
		if tt <= mvcc.TransformingTimetick {
			continue
		}
		mvcc.TransformingTimetick = tt
		cm.vchannelMVCCs[vchannel] = mvcc
	}
}

func isPChannelTransformBarrier(msgType message.MessageType) bool {
	return msgType == message.MessageTypeFlushAll ||
		msgType == message.MessageTypeAlterWAL
}

type vchannelMVCC struct {
	GrowingTimetick      uint64
	TransformingTimetick uint64
}

// VChannelMVCC is a mvcc of one vchannel
// which is used to identify the maximum query-plan timeticks persisted into the wal of one vchannel.
// The state of mvcc is confirmed when both frontiers are covered by the latest
// pchannel-level timetick confirmation barrier.
type VChannelMVCC struct {
	GrowingTimetick      uint64
	TransformingTimetick uint64
	Confirmed            bool
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
