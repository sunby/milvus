package snview

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (h *SNQueryViewHandler) AcquireUpView(ctx context.Context, shardID qviews.ShardID, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	return h.acquireQueryView(ctx, shardID, &version)
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

func (h *SNQueryViewHandler) notifyQueryWaiters(shardID qviews.ShardID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if waiters := h.queryWaiters[shardID.VChannel]; waiters != nil && !h.queriesStopped {
		close(waiters.changed)
		waiters.changed = make(chan struct{})
	}
}

// acquireQueryView waits only when a matching view is UpRecovering. A nil
// version selects the latest Up view and can resolve an unknown replica;
// Phase 2 always acquires the exact replica and version from its query plan.
func (h *SNQueryViewHandler) acquireQueryView(ctx context.Context, shardID qviews.ShardID, version *qviews.QueryViewVersion) (*QueryViewLease, error) {
	var waiters *queryViewWaiters
	defer func() {
		if waiters != nil {
			h.mu.Lock()
			waiters.count--
			if waiters.count == 0 {
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
		// Snapshot before taking shard locks: state changes hold a shard lock
		// while taking h.mu to update indexes and notify waiters.
		var candidates []*snShardView
		if shard := h.shards[shardID]; shard != nil {
			candidates = append(candidates, shard)
		} else if version == nil && shardID.ReplicaID == qviews.UnknownReplicaID {
			for indexed := range h.shardsByVChannel[shardID.VChannel] {
				candidates = append(candidates, h.shards[indexed])
			}
		}
		h.mu.Unlock()

		recovering := false
		var lastErr error
		for _, candidate := range candidates {
			var lease *QueryViewLease
			var pending bool
			var err error
			if version == nil {
				lease, pending, err = candidate.acquireLatestUpView(ctx)
			} else {
				lease, pending, err = candidate.acquireUpView(ctx, *version)
			}
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

func (s *snShardView) acquireUpView(ctx context.Context, version qviews.QueryViewVersion) (*QueryViewLease, bool, error) {
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
	if entry.sm.State() != qviews.QueryViewStateUp {
		return nil, entry.sm.State() == qviews.QueryViewStateUpRecovering, viewerror.NewViewInvalidated("query view %s is not up, current state is %s", version.String(), entry.sm.State().String())
	}
	entry.queryRefs++
	view := proto.Clone(entry.View.IntoProto()).(*viewpb.QueryViewOfShard)
	var once sync.Once
	return &QueryViewLease{
		Version: version,
		Meta:    proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta),
		View:    view,
		Release: func() { once.Do(func() { s.releaseQueryViewLease(version) }) },
	}, false, nil
}
