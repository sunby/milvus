package snview

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (h *SNQueryViewHandler) AcquireUpView(ctx context.Context, shardID qviews.ShardID, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	return h.acquireUpView(ctx, shardID, version, true)
}

// Query execution only needs a reference and metadata; planning also needs an
// owned topology snapshot. Both acquisitions use the same serving lease rules.
func (h *SNQueryViewHandler) acquireUpView(ctx context.Context, shardID qviews.ShardID, version qviews.QueryViewVersion, withTopology bool) (*QueryViewLease, error) {
	return h.acquireQueryView(ctx, shardID, false, func(shard *snShardView) (*QueryViewLease, bool, error) {
		return shard.acquireUpView(ctx, version, withTopology)
	})
}

// queryViewWaiters shares state-change notifications between queries waiting on
// the same vchannel. All fields are protected by SNQueryViewHandler.mu.
type queryViewWaiters struct {
	changed chan struct{}
	count   int
}

// StopQueryAcquisition wakes recovery waiters without releasing resources used
// by in-flight queries. WAL shutdown must call this before waiting for queries
// to finish; CloseForHandoff performs resource cleanup after that wait.
func (h *SNQueryViewHandler) StopQueryAcquisition() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.queriesStopped {
		return
	}
	h.queriesStopped = true
	for _, waiters := range h.queryWaiters {
		close(waiters.changed)
	}
}

// notifyQueryWaiters is called under the changed shard's mutex.
func (h *SNQueryViewHandler) notifyQueryWaiters(shardID qviews.ShardID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if waiters := h.queryWaiters[shardID.VChannel]; waiters != nil && !h.queriesStopped {
		close(waiters.changed)
		waiters.changed = make(chan struct{})
	}
}

// acquireQueryView waits only while a matching view is UpRecovering. Phase 1
// may resolve an unknown replica through the vchannel index; Phase 2 always
// acquires the exact replica and version from its query plan. acquire never
// waits and reports whether recovery can still produce a matching Up view.
func (h *SNQueryViewHandler) acquireQueryView(
	ctx context.Context,
	shardID qviews.ShardID,
	resolveUnknownReplica bool,
	acquire func(*snShardView) (*QueryViewLease, bool, error),
) (*QueryViewLease, error) {
	var waiters *queryViewWaiters
	defer func() {
		if waiters != nil {
			h.mu.Lock()
			waiters.count--
			if waiters.count == 0 && h.queryWaiters[shardID.VChannel] == waiters {
				delete(h.queryWaiters, shardID.VChannel)
			}
			h.mu.Unlock()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h.mu.Lock()
		if h.queriesStopped {
			h.mu.Unlock()
			return nil, viewerror.NewOnShutdownError("query view handler is shutting down")
		}
		var changed <-chan struct{}
		if waiters != nil {
			changed = waiters.changed
		}
		// Snapshot before taking shard locks: state changes and cleanup hold a
		// shard lock while taking h.mu.
		var candidates []*snShardView
		if shard := h.shards[shardID]; shard != nil {
			candidates = append(candidates, shard)
		} else if resolveUnknownReplica && shardID.ReplicaID == qviews.UnknownReplicaID {
			for id := range h.shardsByVChannel[shardID.VChannel] {
				if candidate := h.shards[id]; candidate != nil {
					candidates = append(candidates, candidate)
				}
			}
		}
		h.mu.Unlock()

		recovering := false
		var lastErr error
		for _, candidate := range candidates {
			lease, pending, err := acquire(candidate)
			if err == nil {
				return lease, nil
			}
			if !pending && !viewerror.AsViewError(err).IsViewNotFound() {
				return nil, err
			}
			lastErr = err
			recovering = recovering || pending
		}
		if !recovering {
			if lastErr == nil {
				lastErr = viewerror.NewViewNotFound("query view %s is not found", shardID.String())
			}
			return nil, lastErr
		}
		if waiters == nil {
			h.mu.Lock()
			waiters = h.queryWaiters[shardID.VChannel]
			if waiters == nil {
				waiters = &queryViewWaiters{changed: make(chan struct{})}
				h.queryWaiters[shardID.VChannel] = waiters
			}
			waiters.count++
			h.mu.Unlock()
			// Subscribe only on the slow path, then recheck. A state change
			// between the first scan and subscription must not be missed.
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// acquireUpView also reports whether the exact version is still recovering.
func (s *snShardView) acquireUpView(ctx context.Context, version qviews.QueryViewVersion, withTopology bool) (*QueryViewLease, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return nil, false, viewerror.NewViewNotFound("query view %s is not found", version.String())
	}
	s.advancePendingDownLocked(version, entry)
	if state := entry.sm.State(); state != qviews.QueryViewStateUp {
		return nil, state == qviews.QueryViewStateUpRecovering, viewerror.NewViewInvalidated("query view %s is not up, current state is %s", version.String(), state.String())
	}
	return s.newQueryViewLeaseLocked(version, entry, withTopology), false, nil
}

// Caller holds the shard mutex.
func (s *snShardView) newQueryViewLeaseLocked(version qviews.QueryViewVersion, entry *snViewEntry, withTopology bool) *QueryViewLease {
	entry.queryRefs++
	s.renewServingLeaseLocked(entry)
	// A pending Down command must not appear as the locally leased state.
	meta := proto.Clone(entry.sm.meta).(*viewpb.QueryViewMeta)
	meta.State = viewpb.QueryViewState(entry.sm.coordVisibleState())
	var view *viewpb.QueryViewOfShard
	if withTopology {
		view = entry.sm.buildReport()
	}
	var once sync.Once
	released := false
	return &QueryViewLease{
		Version: version,
		Meta:    meta,
		View:    view,
		Renew: func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !released && !s.closed && s.views[version] == entry && entry.sm.State() == qviews.QueryViewStateUp {
				s.renewServingLeaseLocked(entry)
			}
		},
		Release: func() {
			once.Do(func() {
				s.mu.Lock()
				released = true
				s.mu.Unlock()
				s.releaseQueryViewLease(version)
			})
		},
	}
}

// Caller holds the shard mutex. Timers are only needed after a Down request.
func (s *snShardView) renewServingLeaseLocked(entry *snViewEntry) {
	if s.leaseDuration > 0 {
		deadline := time.Now().Add(s.leaseDuration)
		if deadline.After(entry.leaseExpireAt) {
			entry.leaseExpireAt = deadline
		}
	}
}

// advancePendingDownLocked serializes access, expiry, and Coord's Down intent.
// Active calls protect long-running planning/MVCC waits even past the deadline.
func (s *snShardView) advancePendingDownLocked(version qviews.QueryViewVersion, entry *snViewEntry) {
	if !entry.pendingDown {
		return
	}
	if entry.queryRefs > 0 {
		return
	}
	if remaining := time.Until(entry.leaseExpireAt); remaining > 0 {
		if entry.downTimer == nil {
			entry.downTimer = time.AfterFunc(remaining, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				if s.closed || s.ctx.Err() != nil || s.views[version] != entry {
					return
				}
				entry.downTimer = nil
				s.advancePendingDownLocked(version, entry)
			})
		}
		return
	}
	entry.cancelPendingDown()
	before := entry.sm.State()
	entry.sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	qvobserve.Observe(s.ctx, qvobserve.StreamingNodeApplyCoordViewEvent{ViewStateTransition: qvobserve.ViewStateTransition{CollectionID: collectionIDForEntry(entry), View: entry.View.QueryViewKey(), From: before, To: entry.sm.State()}})
	s.consumeReportPersistAndCleanup(version, entry)
}

func (entry *snViewEntry) cancelPendingDown() {
	entry.pendingDown = false
	if entry.downTimer != nil {
		entry.downTimer.Stop()
		entry.downTimer = nil
	}
}
