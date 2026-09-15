# Cold Search progress and stage attribution

Normal `DefaultBalancer.apply` no longer opens a process-wide dirty-view batch.
Each `AddPreparing` still submits one complete shard event: old views enter
Dropping together with the new Preparing view. The scheduler still serializes
one shard, packs independent shards up to its existing transaction limit, saves
before syncing, and releases references only after durable deletion. Recovery
retains its explicit batch so observers and indexes are installed before replay.

The regression test blocks collection A's DataView pin while collection B reports
Ready. B must persist/enqueue/confirm Up before A is unblocked. Reintroducing the
old Begin/Commit makes this test fail. This proves independence of those lanes;
it does not quantify the latency improvement or catalog write amplification on a
production workload.

## Metrics and populations

Existing metric names and labels are unchanged. New durations use seconds:

- `milvus_qv_stage_duration_seconds{component,operation,stage,result}`
- `milvus_qv_stage_inflight{component,operation,stage}`
- `milvus_qv_stage_items_total{component,operation,stage,kind}`
- `milvus_qv_request_stage_duration_seconds{operation,path,latency_class,stage,result}`
- `milvus_qv_flush_oldest_pending_seconds`

Labels at instrumentation sites are fixed enums. No collection, shard, segment,
node, request ID, error string, or manifest path becomes a metric label. Histogram
result series are created on first observation and then cached. Inflight starts
at Begin, including hangs. Stages that reuse existing completed intervals via
Observe have no inflight series; absence is not zero. `result` distinguishes success, error,
canceled, timeout, superseded, and expected `not_ready` checks. Embedded RPC
Status is inspected; a nil Go error is not sufficient to classify success.

| Component / operation | Stages | Unit and boundary |
| --- | --- | --- |
| Proxy / Search, HybridSearch, Query | request | One logical Proxy method, including readiness and early failure |
| Request cohort histogram | total, readiness, execution | Same completed requests; total = readiness + execution, including zero execution on readiness failure |
| Proxy / auto_load | shared_lifecycle, recheck, load_submit, ready_wait, caller_wait | Lifecycle stages count once per singleflight worker; caller_wait counts each waiting request |
| Coord / readiness | check, wait | Internal readiness result, before it is converted to Status |
| Coord / reconcile | trigger_wait, snapshot, plan, apply, total | Coalesced trigger batch's oldest enqueue; one reconciliation for the others |
| Coord / apply | ensure, prepare, release, shard_lock_wait, shard_lock_hold, pin_dataview, publish_stats | One prepare/release/lock acquisition; hold contains pin and state updates |
| Coord / flush | pending_total, batch_hold, same_shard_wait, eligible_wait | One merged pending shard cohort; original enqueue survives merges; reasons are exclusive |
| Coord / flush | pack, catalog_save, after_persist, sync_enqueue, execute | One claimed batch; catalog_save/views counts attempted persisted entries |
| Coord / view_load | created_to_persisted, persisted_to_all_ready, all_ready_to_up_enqueue, up_enqueue_to_confirmed, total | One locally created view, first boundary only; terminal completion/preemption/release removes state |
| Coord / sync | send_queue, send, callback, apply_report | One pending target until first queue drain; Send is a batch; callbacks are per response |
| Coord / sync_preparing, sync_up, sync_cleanup | roundtrip | First enqueue to matching worker response; includes queueing and worker work; survives reconnect |
| Coord / load_info_watch | build_snapshot, revision_hash, send | Snapshot batch, segment revision, response message respectively |
| QueryNode / load_info | first_snapshot | Registration of a new physical segment through first usable snapshot; includes subscribe and transport |
| QueryNode / view_load | prepare | View resources acquired through local readiness; aborted prepares are counted and removed |
| QueryNode / prepare | collection_runtime, collection_metadata | Collection runtime acquire and metadata load attempt |
| QueryNode / segment_load | total, update_index, reserve, physical_load, release_reservation, on_loaded | One segment load attempt, including canceled tasks that return nil to the scheduler |
| QueryNode / physical_load | new_segment, load_segment, delta, pk | Actually entered phase of one segment load attempt |
| QueryNode / segment_create | pool_wait, collection_lock_wait, collection_lock_hold, convert_load_info, marshal, cgo_create, commit_timestamp, initialize | Each entered call; cgo wall time is not C++ body time |
| Storage / manifest | total, properties, transaction_begin, get_manifest, extract_stats_total | Go/FFI calls, not a claim of pure network time; extract_stats_total contains manifest access |
| StreamingNode / view_load | prepare, catalog_save | View resource acquire to Ready; actual SaveQueryViews call returns before catalog_save ends |
| StreamingNode / prepare | runtime_initialize, data_version | Runtime initialization/version preparation, including errors |
| StreamingNode / bm25_stats | ensure_materialized, shared_wait, load, read, decode | Query-time lazy materialization, caller wait, per-segment load, per-file read/decode; cache/hit and cache/miss count lookups |
| Proxy / shard_query | get_plan, fanout, node_dispatch | Each shard attempt and work-node dispatch; attempt/attempts counts retries too |
| StreamingNode / query_plan | total, lease, mvcc, global_optimize, build_nodes | One plan RPC; global optimizer includes lazy BM25 work |
| QueryNode / query_acquire; StreamingNode / query_acquire | lease, local_optimize, visibility_wait, segment_handles | Each Search/Query acquire path |
| Worker / Search | total, acquire_tasks, execute_tasks | Shared SearchOnView execution code; includes QN and SN, distinguish deployment via target labels |
| QueryNode / search_task | total, prepare_request, segment_search, reduce_total, prepare_export, arrow_export, unattributed | Execution on selected segments; primary sequential phases are prepare_request, segment_search, reduce_total; cleanup/glue is unattributed |
| QueryNode / reduce | heap_merge, marshal_reduce, fill_output_fields, decode_output_fields, encode_result | One entered reduce/output call; multiple slices may run per task |

`path` is the caller's observed readiness state: unloaded, loading, ready,
disabled, unavailable, or unknown. It is not an assertion that the caller won
singleflight or that native caches are warm. `latency_class` is `le1s`/`gt1s`,
chosen from that same request's total at completion. Comparing readiness and
execution for `gt1s` accounts for the whole slow-request cohort.

Different rows have different populations and parallelism. Never sum per-segment
latencies into request latency, sum stage P99s, or average pod P99s. Parent stages
contain children. The same-cohort request phases and the exclusive flush wait
reasons are the intended additive groups. Search task `unattributed` reports
elapsed time outside its three main sequential phases, including cleanup.

## Pending work, request summaries, and limits

All new stage measurements write directly to Prometheus metrics. They cover
operations regardless of whether tracing is configured. The metrics timers do
not create spans or trace links, change trace context, or require an OTel provider.
Existing tracing outside this patch is unchanged. Cross-service per-request
critical paths remain outside this patch's aggregate metrics.

Slow (>1 second) and unsuccessful completions are candidates for a request
summary. A process-wide limiter allows 2/second with burst 10;
`slow_summary/{candidate,emitted,dropped}` exposes its coverage. Summaries do not
replace whole-population histograms or retain every slow request.

View timestamps never reset on repeated progress reports. Recovered views lack
an original Created timestamp and are excluded from new-view latency. A worker
callback can race the return of SyncViews: if boundaries are missing/out of
order, `view_load/coverage/incomplete_order` increments and total is still
recorded; an artificial timestamp is never inserted. This coverage counter is
for completed, locally created views, not all recovered/resident views. Release,
preemption, node failure, coordinator close, and SN handoff clean active timers.
A crashed process cannot emit terminal observations: use inflight/oldest age
before the crash and process restart metrics alongside durations.

The dashboard also references existing native cache load and storage metrics.
Those measure their existing populations and success semantics. Native cache
lookup vs waiting for another loader, pure remote I/O vs FFI/decode, native
manifest column-group access, and client/SDK/network time do not yet have a
complete correlated breakdown in this patch. The native cache implementation
is supplied by milvus-common. A green stage dashboard must not be described as
100% client-to-storage attribution. Use the explicit unattributed interval and
same-window native cache/storage/CPU profiles to identify the remaining work;
extending those native internals needs a separately validated dependency change.

## Verification and rollout

Tests cover independent Ready/Up progress, durable persist-before-sync, deletion
before unpin, same-shard serialization, recovery, pending-cohort timing, repeated
lifecycle events, incomplete boundary order, aborted prepares, embedded RPC
errors, canceled nil-return load tasks, and the same-request phase partition.
The timer benchmark measures a warmed metrics recorder; it does not measure
scrape overhead or full-load impact.

Before evaluating a deployment, import `cold-search-dashboard.json`, select the
Prometheus data source/instances, and retain one identical workload window for
both versions. Compare logical request totals and slow cohorts first, then view
completion, flush write count/batch size, etcd latency, and node loading/execution.
The patch does not deploy itself or produce a new performance result. Native
cache misses at query time and collection readiness are separate axes.
