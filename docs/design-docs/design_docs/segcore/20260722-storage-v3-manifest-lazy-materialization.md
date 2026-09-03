# Storage V3 Manifest Task 与 JSON Stats 延迟物化设计

## 1. 目标

在启用 Loon 的 Storage V3 sealed segment 中，QueryNode 发布 QueryView Ready 状态时，
只为可延迟的 manifest Task 安装轻量字段 facade。以下对象在查询、搜索、索引或大小估算
首次真正需要 chunk 布局或数据时创建：

- projected `ChunkReader`；
- `ManifestGroupTranslator`；
- manifest field-data `CacheSlot`；
- `ChunkedColumnGroup`；
- 真实 `ProxyChunkColumn`。

同一开关也允许将 JSON key stats 顶层对象的远端 `meta.json`、Parquet metadata、
projected reader 和 cache tree 初始化延迟到首个可使用 stats 的表达式。Ready 阶段只发布
绑定当前 generation 的 `LazyJsonStats` facade。

设计边界如下：

- 保持 `SegmentLoadInfo` 已有 Task、projection 和 eager/lazy fallback 粒度；
- 不因开启本功能而按字段重新拆分 Column Group；
- 支持普通字段、RowID、Timestamp、INT64 PK 和 VARCHAR PK；
- Ready 阶段保持 manifest Task 延迟，查询期需要数据时允许正常物化；
- 正确性优先，不增加 Tantivy、PK 或 MVCC 专用的“保冷”执行路径；
- 保持 Storage V1/V2、external collection 和配置关闭时的行为不变。

## 2. 生效条件

Task 仅在以下条件同时满足时进入延迟物化：

1. `queryNode.segcore.tieredStorage.lazyManifestReaderEnabled=true`；
2. segment 的 `StorageVersion=3`；
3. `ManifestPath` 非空；
4. collection 不是 external collection；
5. Task 的 effective field-data warmup policy 为 `disable`；
6. Task 内所有字段均不存在必须在 Load 阶段完成的数据副作用。

一次初始 Load 或 Reopen 在开始时只读取一次配置值，并把该值传给本次并行创建的全部
Task，避免同一 generation 内出现部分 eager、部分 lazy 的混合状态。已发布 Task 不随
动态配置变化而改变。

JSON stats 在前四个条件成立且其 effective scalar-field warmup policy 为 `disable` 时
延迟物化。`sync`、`async`、配置关闭以及非 Storage V3 segment 继续在 Load 阶段初始化，
保持既有行为。

## 3. Task 不变量

Lazy 开关不参与 Task 分组。每个 Task 保持 `SegmentLoadInfo` 已确定的字段集合和
projection：

- 一个多字段 Task 对应一个共享 `LazyManifestColumnGroup`；
- Task 内每个字段对应一个 `LazyManifestProxyColumn`；
- 任一 facade 首次物化时创建该 Task 的完整 projection；
- sibling facade 复用同一个真实 `ChunkedColumnGroup`；
- 既有单字段 fallback Task 继续保持单字段，不与其他 Task 合并；
- PK、Timestamp、RowID 和普通字段不会因为 Lazy 开关而改变分组原则。

## 4. 对象模型

Ready 阶段的对象关系如下：

```text
RuntimeResourceState
├── generation runtime Reader
├── fields
│   └── LazyManifestProxyColumn
│       └── Task-scoped LazyManifestColumnGroup
│           └── ManifestColumnGroupBuildContext
│               ├── generation Reader 强引用
│               ├── 原始 column-group index
│               ├── Task projection
│               ├── FieldMeta 快照
│               └── mmap、priority、cache key 等 Translator 输入
└── json_stats
    └── LazyJsonStats
        └── JsonStatsBuildContext
            ├── generation FileManagerContext
            ├── files、base path、mmap 与 priority
            └── generation insert channel 与 resolved warmup policy
```

每个 Load/Reopen generation 创建并发布自己的 runtime Reader。Lazy Task 直接强引用
该 Reader，不创建额外 Reader 工厂或专用 Reader。首次物化调用：

```text
reader->get_chunk_reader(original_column_group_index, task_projection)
```

旧 PublishedState 中的 facade 继续持有旧 generation Reader，因此 Reopen 后仍在执行的
旧查询不会回查最新 runtime，也不会读取新 generation 的 manifest。

## 5. 物化与并发

`LazyManifestColumnGroup` 保存不可变 build context、已发布 group 和一次构建尝试状态。
物化流程为：

```text
Materialize(op_ctx)
├── 检查 cancellation
├── 同一 Task 进入 single-flight
├── 创建 projected ChunkReader
├── 创建 ManifestGroupTranslator
├── 创建 ChunkedColumnGroup 与 CacheSlot
├── 再次检查 cancellation
└── 成功后一次发布真实 group
```

并发和错误语义如下：

- 同一 Task 的并发首次访问只执行一次构造；
- 等待者可以用自己的 `OpContext` 响应取消；
- 构建者被取消时，不把取消结果固化到 Task，其他请求可以继续构建；
- 非取消失败会返回给本次并发等待者，后续新请求可以重试；
- 失败时不发布半成品 group；
- 成功后所有 facade 复用同一 group。

`LazyJsonStats` 使用相同的 single-flight、独立取消和失败重试语义。成功前不发布
`JsonKeyStats`；原有 `internal_json_stats_latency_load` 只记录真正发生的物化耗时。

## 6. Facade 接口语义

`LazyManifestProxyColumn` 保存共享 Task、FieldId、FieldMeta 和行数。

以下接口不触发物化：

- `NumRows()`；
- `IsNullable()`；
- `IsInMultiFieldColumnGroup()`；
- `CellsLoaded()`；
- 尚未物化时的 warmup cancel 和 manual eviction。

以下接口允许物化：

- `DataByteSize()`；
- `num_chunks()`、`chunk_row_nums()`；
- `GetChunkIDByOffset()`、`GetNumRowsUntilChunk()`；
- `DataOfChunk()`、`Span()`、`GetChunk()`、`GetAllChunks()`；
- `PrefetchChunks()`；
- 批量读取和各类 view 接口。

`CellsLoaded()` 仅表示对应 cache cell 是否已经加载。真实 group 已创建但 cell 尚未加载
时仍可返回 false，因此它可以作为非物化状态探针。

JSON stats facade 的存在性检查只在表达式已满足开关、非空 path 且 path 不含数组下标后
触发物化。首个符合条件的表达式加载 stats，之后同 generation 的查询复用同一实例。

## 7. Lazy 资格

Task 内任一字段具有以下 Load 阶段副作用时，整个 Task 使用原 eager 路径：

- nullable vector 需要建立有效行映射；
- Geometry 字段启用了 Geometry cache；
- struct-array 字段需要建立 array offsets；
- text match 字段需要建立 match index；
- vector 字段可能生成 interim index；
- system field 不属于 RowID 或 Timestamp。

这些场景本来就在 Load 阶段需要数据。直接沿用 eager Task 比新增延迟副作用状态机更简单，
也能保持原有发布语义。

## 8. 系统字段与主键

### 8.1 RowID

RowID 使用普通 manifest facade。Lazy Task 构造的 `ManifestGroupTranslator` 显式允许
读取 RowID；其他 eager 路径继续保持既有的 RowID 忽略规则。需要 RowID 数据时，其原有
Task 正常物化。

### 8.2 主键

Lazy PK 在 Ready 阶段创建 `PkIndexSlot`，但不 Pin cell。该 slot 的 warmup policy
固定为 `disable`，避免全局索引 warmup 在 Ready 阶段反向物化 PK。

`Contain()`、Term、PK iterator 和按 PK 查找首次 Pin slot 时，通过 PK facade 物化原
Task，并使用既有 `PkIndexTranslator` 构建 INT64/VARCHAR PK 索引。读取无关字段不会
为了方便而提前 Pin PK slot。

### 8.3 Timestamp

commit timestamp 为零时，Ready 阶段创建 `TimestampIndexSlot`，但不 Pin cell，也不
创建 `TimestampData` 或 `TimestampIndex`。最大时间戳和 MVCC mask 首次需要索引时
Pin slot；逐行判断需要原始时间戳时直接读取 timestamp facade。

commit timestamp 非零时，可见性语义由该常量直接决定，runtime 不创建 timestamp slot、
owned timestamp data 或 timestamp index。显式访问或 prefetch 原始 timestamp column
时允许物化 manifest Task。

## 9. Reopen 与 generation

Lazy facade 绑定创建时的 manifest generation。Reopen 在 staged runtime 中完成 Reader、
字段 facade、派生 slot、bitset 和内存账本的目标状态，再一次发布。

Column Group identity 包含：

- columns 及顺序；
- format；
- 有序文件列表；
- 文件 path；
- `start_index` / `end_index`；
- 文件 properties。

上述任一项变化时，相关字段创建新 generation Task。物理 identity 未变化时可以继续复用
当前 Task。schema-only drop 只移除被删除字段的 facade，survivor 继续共享原 Task。

生命周期保证：

- 旧 snapshot 可以继续读取旧 generation；
- 新 facade 不捕获 committer、可变 runtime 或当前 PublishedState；
- JSON stats facade 捕获本 generation 的 FileManagerContext、insert channel 与配置；
- Reopen 失败不发布 staged state；
- 同一 generation 内全部新 Task 使用同一个配置快照。

## 10. 内存与大小估算

Lazy facade 在 Ready 阶段不把字段数据计入 segment 的历史 load-time memory stats。
Task 物化后的 cache cell 内存继续由 caching layer 管理，replace/drop 不根据此后变化的
`DataByteSize()` 反向修改旧的 load-time 账本。

`timestamp_data_accounted_bytes` 只记录真正由 segment 持有并计入
`stats_.mem_size` 的 owned TimestampData。Storage V2/V3 pinned data、constant data 和
Lazy slot 均记为零。Reopen 发布时按 old/new runtime 差值调整该账本。

variable field 的平均行大小在首次需要输出大小估算时，可以通过 `DataByteSize()`
物化 metadata 并计算真实值。该调用允许物化，不承诺保持 Task 冷态。

## 11. 配置

```yaml
queryNode:
  segcore:
    tieredStorage:
      lazyManifestReaderEnabled: false
```

配置默认关闭并支持动态刷新。Go paramtable 通过 C bridge 更新 `SegcoreConfig` 中的原子
布尔值。

## 12. 兼容性

- 配置关闭时走原 eager 路径；
- Storage V1/V2 和无 ManifestPath segment 不进入本路径；
- external collection 保持既有加载方式；
- 不修改 milvus-storage API；
- 不改变既有 Task/projection 划分；
- JSON stats 仅延迟顶层初始化，查询执行逻辑与 raw-data fallback 选择不变；
- 查询链路需要 chunk 布局、大小或原始数据时正常物化。

## 13. 验证

单元测试覆盖：

- 配置初始化、动态刷新和关闭兼容；
- Ready 阶段 facade 与未缓存的 PK/Timestamp slot；
- 多字段 Task 的 sibling 共享和并发 single-flight；
- cancellation 与非取消失败后的重试；
- JSON stats 的 Ready 冷态、并发首访 single-flight、取消和失败重试；
- JSON stats 的 `disable`、`sync`、配置关闭与非 Storage V3 边界；
- `DataByteSize()`、布局接口和真实读取的按需物化；
- RowID、INT64/VARCHAR PK、Timestamp 与 commit timestamp；
- Reopen generation 重绑和 schema-only drop；
- Column Group format、columns、文件范围和 properties identity；
- eager blocker 与 warmup 边界；
- 相关查询、PK 和 MVCC 结果正确性。
