# Collection Readiness

Automatic DQL loading previously called `ShowLoadCollections` every 10 ms until
query service was available. Each request had its own polling loop, including
requests for the same collection. This design replaces that loop with one
outstanding `WaitCollectionReady` RPC shared by concurrent requests on a Proxy.

## Contract

`WaitCollectionReady(collectionID, expected_vchannels, check_only)` is an
internal QueryCoord RPC. It never initiates a load.

- A missing desired load config returns `CollectionNotLoaded` immediately.
- Every expected vchannel must have an Up view for every replica in the current
  desired load config. A missing shard counts as unavailable, even if every
  already-registered shard is Up.
- `check_only=true` returns immediately using the same predicate. Otherwise an
  unavailable collection waits for a state change.
- Cancellation, the request deadline, the configured load timeout, runtime
  shutdown, or release ends the wait. A failure of one preparing view does not
  terminate the collection load: the existing balancer may replace that view.

The check is a readiness observation, not a query lease. A release or node failure
can still occur after it succeeds; existing query execution error handling remains
responsible for that race.

## Notification ownership

`qviewsRuntime` owns collection-scoped notifications. `ShardViewRegistry` stats
updates signal only waiters for the affected collection. `LoadConfigStore`
notifies after committed Put/Remove operations; Remove permanently invalidates
existing subscriptions before another Put can commit under the collection guard.
Callbacks perform no RPC or catalog access.

The notifier retains entries only for collections with active waiters. Waiters
share one change channel per collection. Notifications carry no shard, node,
segment, or load-config payload. Releasing a collection retires its subscription
state; a subsequent load gets a different state, so coalescing cannot erase a
release seen by an existing waiter. Runtime shutdown wakes all waits.

## Race-free observation

The RPC subscribes before reading current state, then repeats these steps:

1. Capture the current change channel and check whether the subscription was
   released.
2. Read the immutable load config and its version.
3. Check the complete expected shard set under the registry read lock.
4. Reread the config version. If it changed, retry the observation. Check release
   again before returning readiness.
5. Return if ready, or select on the captured change channel, cancellation and
   shutdown.

A notification before the select closes the captured channel and cannot be lost.
An already-ready collection is detected by the initial read, including recovered
Up views that preceded observer registration. The read path never takes the
collection persistence guard, so a blocked catalog write does not prevent RPC
cancellation. It allocates the expected shard list once per config version and
checks those shards directly, without constructing registry snapshots.

## Proxy sharing

The initial readiness check still executes for each DQL request. When loading is
needed, every caller authorizes its own Load before joining shared work. The
singleflight scope includes load submission and the subsequent readiness RPC.
Callers that observe Loading join the same scope. An individual caller can stop
waiting without canceling the shared operation; the shared operation remains
bounded by the Proxy lifecycle and load timeout.

`ShowLoadCollections` remains available for load-progress queries. The automatic-load
path may still use it through `GetLoadState`, but never periodically while waiting.
Successful sequential DQL requests still perform an initial status RPC; removing
that RPC would require a separately versioned readiness cache and invalidation
protocol.

## Compatibility and validation

The new RPC and request message are appended to the protobuf schema. Existing
field numbers and RPCs retain their meaning. A Proxy requiring this RPC needs a
QueryCoord that implements it; an older server returns Unimplemented. There is
no silent fallback to polling.

Validation covers real SN Up callbacks through the Coord state machine and
registry observer to RPC completion; missing shards and replicas;
notification before blocking; release followed by reload; caller cancellation;
runtime shutdown; a real gRPC deadline and typed released status; concurrent
Proxy callers sharing a single wait; and notifier cleanup under race detection.
Production CPU/alloc savings and mixed-version rollout are not established by
these local tests.
