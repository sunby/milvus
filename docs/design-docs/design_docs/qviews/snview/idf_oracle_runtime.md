# StreamingNode IDF Oracle Runtime Design

> VChannel-level BM25 / IDF resource module for StreamingNode QueryView.
> This document defines the resource lifecycle and preparation flow of
> `IDFOracleRuntime`. Query execution, scoring behavior, and query plan format
> are out of scope.

## 1. Purpose

`IDFOracleRuntime` is the vchannel-level `QueryRuntimeModule` that prepares and
maintains BM25 statistics used by the StreamingNode IDF oracle.

Unlike growing segment data, IDF oracle state is not retained per QueryView
DataVersion. A loaded vchannel owns one `IDFOracleRuntime` inside its singleton
`QueryRuntime`.

The purpose of `IDFOracleRuntime` is to:

1. initialize BM25 statistics from the `VChannelWALView` base DataVersion;
2. fetch sealed BM25 resources from DataCoord for the initialized DataVersion
   when the sealed stats are not initially lazy;
3. continuously generate growing-segment BM25 statistics from live WAL resource
   events forwarded by `QueryRuntime`;
4. record each flushed growing segment's sealed DataVersion;
5. asynchronously advance the current oracle when QueryView reference watermarks
   move forward;
6. atomically apply BM25 statistics diffs so readers never observe a partially
   advanced oracle;
7. clean obsolete growing statistics and sealed local-file references by itself.

`IDFOracleRuntime` is not a live observer. It does not maintain pending buffers
and does not expose catchup state. `QueryRuntime.Initialize` owns buffering,
catchup, and the transition to `Ready`.

## 2. Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `VChannelRecoveryModule` | VChannel-local owner of QueryView references. It creates the vchannel `QueryRuntime`, waits for runtime initialization on `Acquire`, and advances the runtime by oldest active QueryView DataVersion. | It does not compute BM25 stats diffs and does not evict IDF internal segment stats. |
| `QueryRuntime` | VChannel-level singleton runtime. Owns one live-event buffer and one consumer, calls `IDFOracleRuntime.Prepare`, forwards live events, and calls `IDFOracleRuntime.Advance`. | It does not compute BM25 stats or fetch sealed resources directly. |
| `IDFOracleRuntime` | QueryRuntime module that owns one rolling BM25 aggregate, growing BM25 stats store, sealed file references, current DataVersion, and coalesced advance-task state. | It does not own the vchannel live-event buffer, expose external truncation, or own QueryView references. |
| `VChannelWALView` | Provides the initial schema, settings, segment snapshot, historical insert input, and no-gap live resource event stream. | Its capture and no-gap contract are defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `SealedBM25ResourceProvider` | Calls DataCoord to fetch the complete sealed BM25 resource set for a target DataVersion. | It does not cache local files or merge oracle stats. |
| `SealedBM25SegmentCache` | Stores sealed BM25 files on local disk, reuses them across DataVersions, and counts file references. | It does not retain decoded stats, decide DataVersion advancement, or decide contribution membership. |
| `GrowingBM25StatsStore` | Maintains local BM25 stats for growing segments generated from snapshot and live WAL events, plus flushed/sealed metadata. | It does not fetch sealed resources from DataCoord. |
| `IDFAdvanceTask` | Runs on the node-level `NodeScheduler`, serializes asynchronous oracle advancement requests, and coalesces them to the newest allowed requested DataVersion. | One oracle has at most one queued or running task and owns no dedicated advance goroutine. |

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

```text
QueryRuntime.Initialize
        |
        | module.Prepare
        v
IDFOracleRuntime.Prepare
        |
        | sealed resources
        v
SealedBM25ResourceProvider
        |
        v
SealedBM25SegmentCache

IDFOracleRuntime.Prepare
        |
        | growing snapshot stats
        v
GrowingBM25StatsStore
```

Live events:

```text
RecoveryStorage
        |
        | ObserveEvent
        v
QueryRuntime
        |
        | IDFOracleRuntime.ApplyLiveEvent
        v
GrowingBM25StatsStore
```

DataVersion advancement:

```text
VChannelRecoveryModule
        |
        | QueryRuntime.Advance(oldestDataVersion)
        v
QueryRuntime
        |
        | IDFOracleRuntime.Advance(oldestDataVersion)
        v
NodeScheduler / IDFAdvanceTask
```

### 3.2 Runtime State

```text
IDFOracleRuntime
  collectionID
  vchannel
  settings
  currentDataVersion
  currentStats
  currentSealedContributions map[segmentID]*sealedBm25Stats
  currentGrowingContributions set[segmentID]
  preparedSealedContributions map[DataVersion]map[segmentID]*sealedBm25Stats
  growingStore GrowingBM25StatsStore
  sealedCache SealedBM25SegmentCache
  provider SealedBM25ResourceProvider
  pendingDataVersion
  advanceTaskHandle
  close/cancel
```

`currentDataVersion` describes the sealed/growing contribution boundary of the
current oracle. It is advanced by atomic diff commit. Live growing stats may
continue to update while the sealed baseline stays at the same DataVersion.

`currentStats` is the only complete BM25 aggregate. All QueryViews read it,
including QueryViews whose DataVersion is newer than `currentDataVersion`.
Prepared DataVersions retain sealed local-file references only. Growing segments
retain per-segment statistics in `GrowingBM25StatsStore`; sealed statistics are
decoded temporarily when the rolling aggregate is initialized or a diff needs
their contribution.

### 3.3 Contribution Model

For a target DataVersion `D`, the oracle contribution set is:

```text
ContributionSet(D):
  sealed contributions:
    complete sealed BM25 resource set returned by DataCoord for D

  growing contributions:
    local growing BM25 stats whose segment is not in the sealed set for D
    and whose sealedAtDataVersion is absent or > D
```

The sealed set always comes from DataCoord. StreamingNode must not infer sealed
membership for a target DataVersion from local segment metadata alone.

The local `sealedAtDataVersion` is still recorded because it determines when a
growing segment can stop contributing to the oracle and when its local growing
BM25 stats can be removed.

Initial `VChannelWALView` construction contains no flushed segment with an
absent `sealedAtDataVersion`; the owning VChannel module resolves those final
commits before runtime preparation. The `absent` case above is retained for a
live Flush observed after WAL view capture and before its final-commit event is
delivered. Such a segment continues contributing as growing until the exact
first DataView membership version arrives.

### 3.4 Invariants

1. `IDFOracleRuntime` implements `QueryRuntimeModule`.
2. There is only one `IDFOracleRuntime` per loaded vchannel.
3. There is no `DataVersion -> IDFOracle` map.
4. `IDFOracleRuntime` does not own the vchannel live-event buffer.
5. `IDFOracleRuntime` does not expose a module-level catchup handle.
6. Initial construction is triggered by `QueryRuntime.Initialize`, not by
   QueryView `Acquire`.
7. The initialized oracle DataVersion is the `VChannelWALView` base
   DataVersion.
8. Initial sealed BM25 resources are fetched from DataCoord when lazy sealed
   loading is disabled; lazy loading fetches them on the first IDF request.
9. Initial preparation does not use a VChannel-level maximum DataVersion fence;
   it consumes the same per-segment classification as `GrowingRuntime`.
10. Initial growing BM25 stats are generated from the WALView segment snapshot.
11. Live growing BM25 stats are generated from events forwarded by
    `QueryRuntime` in WAL order.
12. The first QueryView `Up` report waits for `QueryRuntime.Initialize` to
    complete successfully.
13. `Advance(oldestDataVersion)` may enqueue asynchronous IDF advancement, but
    QueryView activation does not wait for the advancement to finish.
14. IDF advancement is vchannel-local, serial, asynchronous, monotonic, and
    executed by the node-level `NodeScheduler`.
15. BM25 stats diff is computed outside the commit path.
16. The current oracle is changed only by one atomic diff commit.
17. The runtime owns cleanup of obsolete growing stats, sealed file references,
    and abandoned advance-task resources.
18. A valid live event that cannot be applied is a critical StreamingNode
    corruption, not a recoverable QueryView resource condition.

## 4. Interface Description

### 4.1 QueryRuntimeModule

```go
type QueryRuntimeModule interface {
    Prepare(ctx context.Context, view walview.VChannelWALView) error
    ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)
    Advance(oldestDataVersion qviews.DataVersion)
    Close()
}
```

`IDFOracleRuntime` implements this interface.

### 4.2 IDFOracleRuntime

```go
type IDFOracleRuntime interface {
    QueryRuntimeModule

    // Query-facing BM25 oracle accessors are module-specific and are not part
    // of the lifecycle interface.
}
```

This is an IDF-module concept. The parent `wal/vchannel` package depends only
on `QueryRuntimeModule` and `QueryRuntimeModuleBuilder`; it does not define or
reference `IDFOracleRuntime`.

`Prepare` builds the initial oracle for the provided WALView base DataVersion.
It initializes growing BM25 stats from the WALView segment snapshot. With eager
sealed loading, it also fetches sealed resources and materializes `currentStats`.
With lazy sealed loading, it skips sealed resource discovery until the first IDF
request. `IDFOracleRuntime` keeps the derived oracle state, not the WALView
object passed into `Prepare`.

`ApplyLiveEvent` updates growing BM25 stats and sealed-at metadata from live
events forwarded by `QueryRuntime`.

`Advance(oldestDataVersion)` requests an asynchronous handoff to
`oldestDataVersion` if it is newer than the current oracle DataVersion. If the
target is not newer, it is ignored.

There is intentionally no external `Truncate` method. Obsolete IDF internal
state is cleaned by the runtime after diff commit, segment sealed observation,
or advance-task cancellation.

### 4.3 SealedBM25ResourceProvider

```go
type SealedBM25ResourceProvider interface {
    GetSealedBM25Resources(
        ctx context.Context,
        collectionID int64,
        vchannel string,
        dataVersion qviews.DataVersion,
        partitionIDs []int64,
        loadInfoVersion uint64,
    ) ([]*datapb.StreamingNodeBM25Resource, error)
}
```

The returned resources are the full sealed BM25 resource set for the requested
DataVersion. They are not a diff from StreamingNode's current local cache.

### 4.4 SealedBM25SegmentCache

The cache streams each remote BM25 file to a local segment directory. Acquire
returns a `sealedBm25Stats` object that identifies the local files and owns one
reference. Optional parsing returns temporary decoded stats for initialization.
Cache entries use the deterministic serialization of the complete sealed BM25
resource as their key. Two resources with the same segment ID but different
manifests or BM25 binlog descriptors keep separate local directories.
`FetchStats` decodes local files when an advance diff needs a contribution;
release removes the files only after the current and prepared owners have both
released their references.

### 4.5 GrowingBM25StatsStore

The store records BM25 stats for local growing segments, processes inserts, and
records `sealedAtDataVersion` for flushed segments.
It supplies lightweight membership and temporary changed-segment stats to the
oracle's initialization and diff paths. It is internal to `IDFOracleRuntime`.

### 4.6 IDFAdvanceTask

```go
type IDFAdvanceTask interface {
    Execute(ctx context.Context) error
}
```

`IDFOracleRuntime.Advance` records the greatest pending target and submits at
most one task to the node-level scheduler. Multiple requests may be coalesced as
long as the task never commits an oracle newer than the latest allowed
`oldestDataVersion` observed from the resource manager. If a newer target
arrives during execution, the task returns `nodescheduler.ErrDelay` after the
current commit and moves to the scheduler queue tail. A failed diff computation
restores its target as pending and returns a delayed error for retry.

There is no dedicated goroutine, notification channel, or worker per vchannel.

## 5. Actual Behavior

### 5.1 Initial Preparation

```text
QueryRuntime.Initialize
  -> IDFOracleRuntime.Prepare
  -> fetch sealed BM25 resources for base DataVersion if not initially lazy
  -> store sealed files locally and materialize currentStats if not initially lazy
  -> build growing BM25 stats from WALView segment snapshot
  -> assemble current oracle
```

The initial oracle DataVersion is the `VChannelWALView` base DataVersion.
With lazy sealed loading, `currentStats` is materialized on the first IDF
request. Before that happens, `PrepareDataVersion` accepts newer DataVersions
without sealed-file access or retained per-version state. After the first
materialization, preparation downloads target sealed files before the existing
QueryView Ready callback without decoding them or building another aggregate.
The rolling aggregate advances when the oldest retained QueryView DataVersion
moves forward.
Lazy sealed loading does not suppress WALView reads of persisted growing BM25
stats, which may still access object storage during initial preparation.

### 5.2 Live Event Apply

```text
QueryRuntime.applyLiveEvent(event)
  -> IDFOracleRuntime.ApplyLiveEvent(event)
  -> GrowingBM25StatsStore.ApplyLiveEvent(event)
```

Live events update growing BM25 stats and record flushed segment
`sealedAtDataVersion`. `QueryRuntime` owns ordering and ensures the same event
sequence is also applied to the other resource modules.

### 5.3 First QueryView Up

The first QueryView `Up` report waits for `QueryRuntime.Initialize` to complete
successfully, not for an IDF-specific catchup handle.

`QueryRuntime.Initialize` returns successfully after:

1. `GrowingRuntime.Prepare` returns;
2. `IDFOracleRuntime.Prepare` returns;
3. `QueryRuntime` starts its singleton consumer;
4. `QueryRuntime` atomically takes the current live-event buffer batch;
5. every event in the initial batch has been applied to both modules.

### 5.4 Oracle Advancement

```text
QueryView references move forward
  -> VChannelRecoveryModule computes oldestDataVersion
  -> QueryRuntime.Advance(oldestDataVersion)
  -> IDFOracleRuntime.Advance(oldestDataVersion)
  -> coalesce pending DataVersion
  -> NodeScheduler.Submit(IDFAdvanceTask), at most once per oracle
```

If the target is newer than the current oracle DataVersion, the task computes
and commits a BM25 diff asynchronously. QueryView activation does not wait for
this background handoff.

The diff model:

```text
negative contributions:
  sealed segments that leave the target contribution set
  growing segments with sealedAtDataVersion <= target DataVersion

positive contributions:
  sealed BM25 resources returned by DataCoord for the target DataVersion
  growing segment stats still not covered by the target sealed set
```

The task compares sealed resource keys and decodes changed sealed contributions
outside the commit path. During the atomic commit it derives growing membership
and changed growing contributions from the latest `GrowingBM25StatsStore`
state, then applies the sealed and growing changes to the current oracle. Live
growing updates and the commit are serialized by the oracle lock.

### 5.5 Cleanup

After an advance commit, `IDFOracleRuntime` releases sealed file references and removes
growing BM25 stats that can no longer contribute to any future oracle state.

Cleanup is internal. The resource manager never calls an IDF-specific truncate
operation.

### 5.6 Close

`Close` marks the oracle closed, cancels and waits for its scheduled task,
releases current and prepared sealed file references, closes the
growing stats store, and makes the oracle unavailable. It is called only by
`QueryRuntime.Close`.
