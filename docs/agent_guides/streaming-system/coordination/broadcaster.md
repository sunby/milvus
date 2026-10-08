# Broadcaster

Executes cross-PChannel atomic broadcast for DDL/DCL messages with resource locking, ACK tracking, and callback execution. Singleton running inside StreamingCoord.

## Broadcast API

Callers use `broadcast.StartBroadcastWithResourceKeys(ctx, resourceKeys...)` to obtain a `BroadcastAPI`, which acquires resource key locks and returns after WAL-based DDL is ready. The caller then constructs a `BroadcastMutableMessage` with target VChannels (must include CChannel) and calls `Broadcast()`. `Close()` releases locks if no broadcast was issued.

Non-primary clusters reject all broadcasts with `ErrNotPrimary`.

## Broadcast Flow

1. **Lock**: Acquire ResourceKey locks in sorted order (Domain, then Key). SharedCluster is added automatically.
2. **Persist**: Allocate BroadcastID, create task in PENDING state, persist to catalog. Once persisted, the broadcast is guaranteed to eventually complete even across crashes.
3. **Append**: `broadcastScheduler` dispatches the task to a worker that calls `AppendMessages()` to write to all target PChannels.
4. **FastAck**: If `AckSyncUp` is not set, the broadcaster immediately self-acks all VChannels using the append results (no need to wait for consumer-side ACK). Otherwise, waits for StreamingNode consumers to ACK each VChannel.
5. **AckCallback**: CChannel ACK enqueues the task into `ackCallbackScheduler`. The callback executes only after all VChannels are ACKed. For tasks with conflicting ResourceKeys, callbacks execute in CChannel TimeTick order. Callbacks retry with exponential backoff until success.
6. **Tombstone & GC**: After callbacks complete and TOMBSTONE is persisted, release the callback resource locks before handing the task to `tombstoneScheduler`. Handoff appends to an in-memory queue and coalesces wakeups; catalog deletion runs outside the queue lock. Every queued ID is retained until drained. Recovery rebuilds this queue from durable TOMBSTONE tasks if shutdown interrupts handoff. GC applies the existing count and lifetime limits; a sustained deletion deficit can still grow the queue.

GC removes eligible tombstones in batches bounded by `metastore.maxEtcdTxnNum` (default 64), using exact broadcast-task keys. Each successful batch retires its in-memory tasks and advances the queue; failures retain the batch for idempotent retry, including when the deletion result is ambiguous. Late ACKs on TOMBSTONE or DONE tasks are ignored, so GC deletion holds neither task nor manager locks. Manager shutdown cancels an in-flight deletion; recovery only enqueues records still present in the catalog and does not replay their completed callbacks.

## Resource Key Locking

Each ResourceKey has: **Domain** (resource type), **Key** (entity identifier), **Shared** (read vs exclusive). Every broadcast automatically acquires SharedCluster.

Domains: `Cluster`, `DBName`, `CollectionName`, `Privilege`, `SnapshotName`.

See [Message Semantic Docs](../message/message.md) for per-message ResourceKey usage.

## BroadcastTask State Machine

```
PENDING → TOMBSTONE → DONE (removed from catalog)
REPLICATED → TOMBSTONE → DONE (removed from catalog)
```

- **PENDING**: Created, awaiting WAL append and ACK. After append, FastAck self-acks all VChannels immediately (unless `AckSyncUp` is set, in which case waits for consumer-side ACK).
- **REPLICATED**: Task created on secondary cluster from replicated ImmutableMessage (no resource lock held). Execution order guaranteed by CChannel TimeTick ordering in `ackCallbackScheduler`.
- **TOMBSTONE**: All ACK callbacks complete, resource locks released. Awaiting GC.
- **DONE**: Removed from catalog.

## Key Packages

- `internal/streamingcoord/server/broadcaster/` — `Broadcaster`, task scheduling, resource locking, ACK callbacks, singleton accessor
