# LoadCollection 请求级强制同步预热设计

状态：实施中。首轮代码已落地到本地工作区，尚未完成 native / E2E 验收，功能门禁默认关闭。实施和验证结果见附录 B；不能将本文的目标契约等同于已经验证的保证。

日期：2026-09-07。源码基线：`19c753b4d180bb3f2581c2f7a58d4c4ac06a8202`。

适用路径：当前分支的 QueryView / Streaming DDL 加载链路，以及 QueryNode 的统一 segment Load / Reopen 路径。本文以当前源码为准；旧设计说明仅用于理解背景，不覆盖源码事实。

## 1. 结论与设计选择

基线版本（不含本次实施补丁）不能通过公开的 `LoadCollection` 参数，可靠地要求一个使用 `warmup=disable` 的 collection 执行强制同步预热。

虽然内部已经存在多个 `ForceSyncWarmup` 字段和部分旧加载路径的处理逻辑，但公开请求、持久化加载配置、QueryView、统一 segcore 加载之间没有形成完整闭环。仅在 QueryCoord 设置一个 bool，不能实现本需求。

推荐提供如下能力：

- 用户首次加载时指定 `warmup="sync"`，覆盖本次加载生命周期内、实际加载资源的 warmup 策略，包括配置中的 `disable` 和 `async`。
- 未指定参数时，保留现有默认行为；对已启用的 collection，省略参数不撤销同步预热要求。
- 配置持续到 `ReleaseCollection`，后续新增 segment、节点迁移、恢复、索引更新均继承这一要求。
- 只有实际需要的资源完成同步预热后，才能发布相应的 loaded / Ready 状态。
- 不修改全局 warmup、lazy、mmap 配置，不修改持久化 schema / index properties。
- 第一版不支持对已按普通模式加载的 collection 原地开启；需要 Release 后重新 Load。
- 为防止 Release / Reload 和旧 segment 复用绕过完成条件，增加持久化的 `sync_warmup_epoch`，并检查资源的实际完成资格。

这里的“sync”指预热完成被纳入 load-ready 条件，不是把公开 LoadCollection RPC 改成一直阻塞到整个 collection 加载完成。

## 2. 背景：warmup 与 lazy 是两个不同层次

### 2.1 warmup 的职责

warmup 控制是否主动填充 cache cell，以及何时等待完成：

| 策略 | 加载期间行为 | Ready 与预热的关系 |
| --- | --- | --- |
| `disable` | 不主动 warm cache cell；使用时再按需读取 | 不等待主动预热 |
| `async` | 提交后台预热 | 通常不以后台预热完成作为 loaded 条件 |
| `sync` | 在加载路径中执行、等待预热 | 预热成功后才能完成对应加载步骤 |

在本次检查的 cachinglayer 头文件中，`CreateCacheSlot()` 会调用 `Warmup()`；同步分支通过 pin 所有 cell 等待加载完成。`Warmup()` 带有只执行一次的状态，即使第一次策略为 disable，也不能假设再次调用它就会补做同步预热。

这些事实不等于“所有数据永久在内存”。预热后的 pin 可以释放，资源仍遵守现有的内存 / 磁盘缓存、mmap 和 eviction 机制。

### 2.2 lazy 的职责

lazy 控制 reader / translator 或部分 JSON stats 元数据是否延迟构造，不能与 `warmup=disable` 等同。

例如，Storage V3 manifest 字段进入 lazy 路径，还需要同时满足 lazy 开关开启、manifest 可用、非 external collection、有效 warmup 为 disable，以及没有必须在 load 阶段执行的副作用等条件。

当 manifest lazy 开关关闭时，warmup 仍可以是 disable：加载时创建 reader / translator / slot，cache cell 留到查询时读取。这是“提前创建对象、延迟加载数据”，不是 lazy translator materialization。

JSON stats 使用独立的 `lazyJsonStatsEnabled`。其延迟初始化也要求有效 warmup 为 disable；不能用 manifest lazy 开关代替这个独立配置。

因此，force sync 的正确实现不是关闭全局 lazy 开关，而是先计算请求的有效 warmup，再让相关资源自然退出 deferred 分支。`LazyJsonStats` 包装对象仍可保留，但其所需内容必须在加载完成前 materialize。

### 2.3 当前 YAML 与代码缺省值必须区分

源码基线中的两层配置如下。这里不是线上运行时配置的查询结果。

| 配置后缀，前缀为 `queryNode.segcore.tieredStorage.` | 仓库 YAML 显式值 | paramtable 缺省值 |
| --- | --- | --- |
| `warmup.scalarField` | `disable` | `sync` |
| `warmup.scalarIndex` | `disable` | `sync` |
| `warmup.vectorField` | `disable` | `disable` |
| `warmup.vectorIndex` | `disable` | `sync` |
| `lazyManifestReaderEnabled` | `false` | `false` |
| `lazyJsonStatsEnabled` | `true` | `true` |

实际配置还取决于加载的配置文件和覆盖项。本文的目标是：无论该 collection 原本得到 disable、async 还是 sync，显式 force sync 都对其加载范围内的资源生效。

## 3. 目标、范围与非目标

### 3.1 第一版目标

1. 在公开 LoadCollection 请求中表达同步预热要求。
2. 保留该要求，直到对应 collection 被 Release。
3. 完整覆盖当前统一加载链路，而不是只覆盖旧版 `LoadFieldData` / `LoadIndex` 调用。
4. 阻止 lazy 分支、旧 segment 复用、迟到任务回调绕过同步完成条件。
5. 将失败保留为加载失败 / 未完成，不返回“已完成”来掩盖预热失败。
6. 不影响其他 collection 的缓存策略和查询行为。

### 3.2 支持范围

支持普通内部 collection 的 sealed segment。Storage V1 / V2 / V3 的适用资源均须验收，重点验证 Storage V3 manifest 路径。

只预热当前 load plan 实际选择的资源表示：

- 已选择加载的字段数据 / column group。
- 实际加载的向量索引、标量索引、text-match 资源。
- 已启用且需要加载的 JSON stats，包括其下游的数据和索引资源。
- 当前加载路径需要的 PK / timestamp 等派生结构。

不因为 force sync 额外选择字段、创建用户未配置的索引、打开原本禁用的 JSON 功能，或强行再加载一份已由索引提供的冗余 raw data。column group 粒度导致的必要读取放大属于底层存储布局，不是扩大公开 `load_fields`。

External collection 第一版明确拒绝该参数；其 reader、外部文件缓存和生命周期需要单独定义与验收，不能静默忽略。

### 3.3 非目标

- 不提供独立的 Prewarm API，也不替代 namespace 的临时 Prewarm。
- 不支持普通已加载 collection 的原地升级，也不提供单次调用后自动恢复旧 warmup 的语义。
- 不保证缓存永久驻留，不关闭 eviction，不保证所有 mmap 页已经物理驻留。
- 不承诺首个查询完全无远程 I/O；例如绕过该缓存路径的 take / output 路径不在此保证之内。
- 不承诺用户未选择的字段、未来尚未产生的数据已经预热。
- 不改变 growing segment、L0 / delta replay 的原有正确性条件。
- 不扩展公开 LoadPartitions 的能力；内部历史字段只做语义隔离与回归。
- 不借此重写整个 QueryView 调度器、错误码体系或存储缓存系统。

## 4. 当前代码的关键事实与缺口

| 环节 | 当前代码事实 | 本需求的缺口 |
| --- | --- | --- |
| 公开协议 | `milvuspb.LoadCollectionRequest` 已有 `load_params` map | 可以复用 map，但当前没有完整 warmup 参数语义 |
| Proxy | `loadCollectionTask` 读取 `load_priority`，构造内部请求 | 没有解析并传递请求级 sync warmup |
| QueryCoord | LoadCollection 构造 expected config 时 `ForceSyncWarmup: false` | 不能按用户请求形成持久加载要求 |
| LoadConfig | 当前持久化 / Clone / recovery 模型没有新的长期 sync 策略 | 重启、新 segment、重复 Load 无法可靠继承 |
| segment watch | 获取 segment load info，再计算快照 revision | 需要合并策略且先于 hash，不能旁路 revision |
| QN 统一加载 | physical loader 创建 segment，再等待 `segment.Load().Await()` | 不是只调用旧的字段加载 helper |
| Go → segcore | `ConvertToSegcoreSegmentLoadInfo` 没有复制 force 标记 | 内部 querypb 有字段，也不能自动到达 C++ |
| segcore proto | 当前 `SegmentLoadInfo` 到字段 24，没有 force 标记 | 需要新增并贯穿 Load / Reopen |
| runtime compact | Go 与 C++ 均有手工重建 load info 的逻辑 | 新标记可能在压缩后丢失 |
| 资源复用 | physical manager 和 transform readiness manager 都有已加载快捷路径 | 仅新建资源执行预热，不能证明复用资源符合要求 |
| 公开加载进度 | `qviewsLoadPercentage()` 统计 registry 中已有 shard 的 UpVersion | 未创建的目标 shard 不在分母里；还需补完整目标与 epoch 检查 |
| Release | 公开 RPC 提交 release 后返回，底层清理由后续流程执行 | RPC 返回不能视为所有旧资源已消失 |

旧 helper 中已经有 `ForceSyncWarmup` → `sync` 的处理，不代表统一路径已经支持本功能。需要逐个验证输入从哪里构造、经过哪些转换、最终被哪个 translator 使用。

### 4.1 与旧设计说明不同的部分

旧 shard-view 说明中存在“manager 保留到 registry 关闭”等描述；当前代码的实际行为是：最后一个 view 完成 durable removal 后，`finalizeRemoval()` 释放 DataView 引用，再通过 `removeEmptyManager()` 回收空 manager 及其反向索引。

本文采用该实现事实。与此同时，QN 的 `detachViewLocked()` 在 `queryRefs > 0` 时仍保留 transform segment。因此，即使协调端的旧 view 已清理，也不能把所有 QN 中的“同 segment ID 已加载”当成新一次 Load 已经完成。

## 5. 对外 API

### 5.1 参数定义

在公开 `LoadCollectionRequest.load_params` 中增加受支持的 key：

```text
warmup = sync
```

第一版只接受字符串 `sync`。参数不区分内部实现细节，不向客户端暴露 epoch。

| 输入 | 行为 |
| --- | --- |
| 不提供 `warmup` | 首次 Load 沿用现有策略；已有 sync 配置保持不变 |
| `warmup="sync"` | 申请持久到 Release 的同步预热策略 |
| 空串、`async`、`disable`、大小写变体或其他值 | 参数错误，不静默回退 |
| `refresh=true` 且显式 `warmup="sync"` | 参数错误；refresh 不承担开启 / 重做预热的语义 |
| external collection 且显式 sync | 明确返回不支持 |

不提供 `warmup="disable"` 来关闭策略，避免“持久 load 配置”与“单次请求覆盖”混用。关闭方式是 Release 后普通 Load。

### 5.2 SDK 形态

下列示例均是拟新增接口，不是当前已支持的调用。

PyMilvus：

```python
client.load_collection(
    collection_name="items",
    warmup="sync",
    timeout=300,
)
```

SDK 将参数写入 `load_params["warmup"]`，并沿用现有的同步等待逻辑。不能只是接收一个 `**kwargs` 后不写入请求。

Go SDK：

```go
task, err := client.LoadCollection(ctx,
    milvusclient.NewLoadCollectionOption("items").WithSyncWarmup())
if err != nil {
    return err
}
return task.Await(ctx)
```

这里 `LoadCollection` 仍返回任务；调用方通过 `Await` 等待加载进度完成。

REST V2 在 load endpoint 的请求体增加 `warmup: "sync"`，转成同一个 protobuf map。应使用 load 专属请求结构，避免将参数无意加入所有共用 `CollectionNameReq` 的 endpoint。旧 REST 路径保持现有行为；只有显式完成适配、不会静默吞参的入口才宣称支持。

### 5.3 三种“完成”必须区分

| 事件 | 本设计的含义 |
| --- | --- |
| LoadCollection RPC 成功 | 加载要求已接受 / 提交；不等于所有数据已预热 |
| SDK 同步等待或 LoadTask.Await 成功 | 对应 load-ready 条件达到 100%；其中包含本设计要求的同步预热 |
| 首次 Search / Query 返回 | 正常查询完成；不作为预热完成的补充条件 |

PyMilvus 的异步选项继续表示不等待加载完成，不会把 warmup 策略改成 async。

同步等待超时不隐式 Release，也不回滚已经持久化的策略。任务是否继续由现有加载生命周期决定；客户端可以查询状态或显式 Release。

现有 SDK 按 collection 查询进度，而非按请求 ID 等待。本设计不新增公开 operation handle；另一个调用方并发 Release / Reload 会改变等待对象的现态，不能承诺原请求拥有独立于 collection 生命周期的完成结果。

## 6. 策略优先级与预热范围

先决定资源是否属于 load plan，再决定它的 warmup，顺序不能颠倒：

```text
不属于加载范围 / 被现有规划判定为不需要：维持跳过
属于加载范围，且该加载生命周期 sync_warmup=true：sync
否则：完全沿用现有资源专属的策略解析规则
```

这不是修改通用 `getCacheWarmupPolicy()`，使所有 `in_load_list=false` 都变成 sync。该函数现有的范围保护必须保留。

普通模式下的字段 / 索引 / collection / 全局优先级并不完全统一，尤其是系统字段以及“向量字段没有可用索引”的分支。实现不能凭空替换成一条统一的旧策略排序。新增 force 判断应放在各资源已有解析入口的前端，并保留非 force 分支。

| 对象 | force sync 时的要求 | 不能做的事 |
| --- | --- | --- |
| 普通字段 / manifest group | 该计划实际加载的数据使用 sync，不能留在 deferred reader 状态 | 不加载排除字段或所有 manifest 列 |
| 无可用索引的 raw vector 分支 | 检查其特殊策略入口，同样接受 force | 不仅覆盖常规 vector-field 配置入口 |
| 向量索引 | 外层 translator 与内部索引加载配置均接受 sync | 不把外层 metadata 已加载视作内部 cell 已热 |
| 标量索引 / text match | 在 index info 构造和 translator 创建前注入 sync | 不漏掉不经过通用字段 resolver 的资源 |
| JSON stats | 不延期 materialize；将 sync 传到下游 column group / shared-key / shredding 资源 | 不只加载包装对象，也不强制启用禁用的功能 |
| PK / timestamp 派生结构 | 对实际需要的派生 slot 同步预热并等待 | 不假设原字段已加载就等于派生 slot 已加载 |
| commit timestamp 常量等优化 | 保留无需建索引的优化 | 不为 force 人为创建不必要资源 |
| 多字段同一 load task / group | force 为 sync；普通模式保留既有策略聚合 | 不把 group 聚合解释成字段级并行承诺 |

## 7. 状态模型与协议字段

### 7.1 为什么要独立的长期配置

当前内部 `ForceSyncWarmup` 还用于 namespace Prewarm，存在任务完成后清除临时标记的逻辑。复用该临时状态作为 LoadCollection 的持久策略，会导致长期要求被清掉。

因此区分三类状态：

- `sync_warmup`：collection 加载生命周期的持久要求。
- `sync_warmup_epoch`：该同步加载生命周期的唯一标识。
- `force_sync_warmup`：某次 segment 执行输入的有效要求，可由持久策略或原有临时路径导出。

epoch 不表示 cache 热度，不是 segment metadata revision，不是 DataView version。

### 7.2 建议字段

下表字段号根据当前基线的空位提出，实施时必须重新检查 reserved / 已分配字段，不手工编辑生成文件。

| 位置 | 拟新增 / 复用字段 | 用途 |
| --- | --- | --- |
| 公开 milvuspb LoadCollectionRequest | 复用 `load_params["warmup"]` | 客户端表达，无需为本参数修改公开 proto |
| querypb LoadCollectionRequest | `bool sync_warmup = 11` | Proxy → QueryCoord |
| AlterLoadConfigMessageHeader | `bool sync_warmup = 9`；`int64 sync_warmup_epoch = 10` | WAL 中传递持久加载策略 |
| querypb CollectionLoadInfo | `bool sync_warmup = 12`；`int64 sync_warmup_epoch = 13` | ETCD 保存和恢复 |
| loadmgr.LoadConfig | `SyncWarmup bool`；`SyncWarmupEpoch int64` | 内存不可变快照、Clone、比较 |
| viewpb.QueryViewMeta | `bool sync_warmup = 8`；`int64 sync_warmup_epoch = 9` | 每个 view 的不可变预热要求 |
| querypb.SegmentLoadInfo | 复用现有 `force_sync_warmup = 31` | 具体执行输入的 force 要求 |
| segcorepb.SegmentLoadInfo | `bool force_sync_warmup = 25` | C++ Load / Reopen 执行输入 |
| QN 任务 / 物理资源状态 | 本地的 epoch、attempt / generation、applied revision 和完成资格 | 防止复用及迟到回调越过完成条件 |

epoch 随 QueryView 到达 QN，不需要由 C++ 解释。C++ 只需知道本次加载是否必须同步预热；QN 在该次加载真正完成后记录资格。

暂不把长期策略重复持久化到 PartitionLoadInfo。它属于 collection 加载生命周期，避免 collection 与 partition 两份状态分叉。已有临时字段保留其原有职责。

### 7.3 epoch 的产生与恢复

- 首次创建 sync 加载配置时，使用 QueryCoord 现有持久 ID allocator 分配一个非零 ID。
- 在生成 AlterLoadConfig 消息前分配，并随消息一起持久化；ACK 重放使用消息中的值，不能重分配。
- 同一生命周期重复 Load、调整 replica / resource group、后续 segment 变化保留 epoch。
- Release 删除该配置；下一次首次 sync Load 分配新 epoch，即使 collection ID、segment ID 都相同。
- QueryCoord / StreamingNode / QueryNode 恢复时，按各自职责恢复要求；资源完成资格必须由真实加载建立，不能仅从 proto 恢复为“完成”。
- `sync_warmup=true && epoch=0` 视为不完整的内部状态，拒绝作为 ready 依据。普通 / 旧配置使用 `false, 0`。

不能直接使用 `LoadConfigStore.ConfigVersion()` 作为该 epoch：当前恢复逻辑重建内存版本，不能保证跨重启、删除重建后仍具有加载生命周期的唯一性。

## 8. 端到端传递与完成顺序

| 阶段 | 处理 | 必须保持的不变量 |
| --- | --- | --- |
| SDK / REST | 解析 / 编码 warmup 参数 | 参数不得被静默丢弃 |
| Proxy | 校验参数、refresh 冲突、入口支持范围，转内部 bool | 无参数路径保持兼容 |
| QueryCoord DDL | 在 collection 串行化范围内检查现态，生成策略和 epoch | 普通已加载状态不可被隐式升级 |
| WAL / ACK | 持久化完整 load config，再触发后续调度 | 重放、Clone、恢复不丢字段 |
| Balancer | 从同一配置快照构造 load-info version 和 view warmup 要求 | 不把不同快照字段拼成一个 view |
| Segment watch | 克隆 load info，注入有效 force，再计算 revision | revision 覆盖实际发布的 payload |
| QN 任务 | 用 view 要求约束本次执行；绑定 epoch 和 attempt | 旧结果不能完成新任务 |
| Go / C++ | 序列化 force，按资源解析 sync，执行同步预热 | Load / Reopen / compact 全部保留要求 |
| QN Ready | 预热完成资格通过，再满足原有 transform catchup 等条件 | 任一必要条件未达成，不报告 Ready |
| QueryCoord / SDK | 既有 view 发布与加载进度链路消费上述结果 | 100% 不得早于需要的同步预热完成 |

逻辑完成顺序为：执行所需资源的同步预热成功 → segment 加载成功 → transform 等原有准备条件满足 → QN Ready → 协调端可发布 view / 更新加载进度 → SDK 等待成功。

这是一组条件依赖，不要求原有可并行步骤改为串行。SDK 等待期间发生新的目标变更，沿用既有进度语义；本设计不引入“未来再也不会出现新 segment”的全局静止点。

### 8.1 公开进度还需要一个完整性门禁

当前进度链为 Proxy `GetLoadingProgress()` → `getCollectionProgress()` → QueryCoord `ShowLoadCollections()` → `qviewsLoadPercentage()`。

当前实现只对 registry 已出现、replica ID 匹配的 shard 计数：`loaded / total`，其中 `UpVersion != nil` 即计为 loaded，`total == 0` 返回 0。它没有独立检查预期 shard 集合是否已经全部建立，也没有本提案的 epoch 检查。

因此，仅加强 QN Ready 仍不能构成完整证明：如果预期有两个 shard，但 registry 暂时只出现一个已 Up 的 shard，按这段函数的局部输入会得到 100%。实际是否出现该交错取决于调度过程，不能将其不存在当作接口保证。

对 sync collection 增加以下进度规则，普通 collection 仍走现有逻辑：

1. 由 LoadManager / 协调运行时维护只读的目标完整性快照，绑定 collection ID、load-config version 和 epoch。
2. 从权威 collection 元数据中的 virtual channel 集合，以及同一 load config 的 replica 集合，构成 `ExpectedShards = VChannels × ReplicaIDs`。不要从“当前已创建的 manager”反推目标。使用已有 DescribeCollection 结果或恢复时重新读取并缓存，不在每次轮询中做远程 metadata 请求。
3. 快照尚未初始化，或初始化期间配置已经变化时，不允许返回 100%。恢复先重建目标，Release / 配置替换时使旧快照失效。
4. 一个 shard 只有存在符合当前 sync epoch 的 Up view 才计为完成。为此 stats 投影需携带 Up view 的 sync / epoch 信息，不能只保留 UpVersion 后丢失约束。
5. 分母使用完整 ExpectedShards，缺失 manager / view 计为未完成。空数据 collection 也要等待其正常的空 shard view 完成准备，不能以没有 segment 为由提前完成。

这不是提供字节级预热百分比，而是让现有 collection 加载百分比具有可信的 100% 边界。后续 Reopen 保持旧 Up view 可服务时，普通 load progress 可以仍为 100%；它不表示该次 metadata 更新已经完成，更新的完成边界仍由 staged Publish / 新 view 保证。

## 9. QueryCoord 生命周期、幂等和并发

### 9.1 请求状态表

| 当前持久配置 | 请求 | 结果 |
| --- | --- | --- |
| 无，旧资源已可安全隔离 | 省略 warmup | 普通 Load，保持当前行为 |
| 无，旧 view 已排空 | sync | 分配 epoch，建立 sync 配置 |
| 无，但上一轮 view 仍存在 / 等待 durable removal | sync | 返回可重试的 not-ready；不假装是全新加载 |
| 普通加载中 / 已加载 | 省略 warmup | 现有幂等 / 参数变更规则 |
| 普通加载中 / 已加载 | sync | 明确状态冲突，要求 Release 后重载 |
| sync 加载中 / 已加载 | 省略 warmup 或 sync | 保留策略及 epoch；其他参数仍按现有规则校验 |
| sync | 不带 warmup 的 refresh | 保留策略；refresh 新产生的加载工作继承 sync |
| sync | Release | 按原流程删除配置、推进资源清理 |

重复 sync Load 不是“再预热一次所有已被 eviction 的数据”。它是声明同一生命周期仍要求同步加载。需要重新执行整轮加载时，使用 Release / Load。

### 9.2 Release 排空检查

新 sync 生命周期在协调端的准入，需要检查该 collection 的旧 QueryView 是否均已完成 durable removal。

不能用 `Up == nil`、segment 数量为零或 active stats 不再包含 collection 来替代排空：当前 stats 会排除部分 Dropping / Dropped 状态。

建议提供内部只读的、锁语义明确的 `HasUndrainedViews(collectionID)` 查询，检查真实的 manager view / pending-removal 状态。不要把“registry 有一个空 manager”直接视作未排空，也不要跳过尚未完成持久删除的 view。

该检查不会要求等待所有用户查询终止。可能被旧查询持有的资源由 QN epoch 校验隔离；否则 Release 之后的长查询会变成隐式全局 barrier。

### 9.3 不能忽略旧调度计划

当前 `DefaultBalancer.apply()` 对已有 plan 调用 `RequestRelease()` / `AddPreparing()`，本身没有重新检查 load config version。仅在 API 入口检查旧 view 为空，仍可能遇到旧快照计划迟到提交。

本设计需要一个 collection 级的 apply fence：

1. 调度计划保存它观察到的 load-config version / 预期存在性；sync plan 同时保存 epoch。
2. apply 时在与 load-config Put / Remove 协调的 collection guard 下重新验证。
3. prepare 不匹配当前配置、replica 或 epoch 时丢弃，触发重新规划。
4. release 计划也校验预期状态，不能让上一轮 release 删除下一轮已经建立的 view。
5. 准入排空检查与状态读取使用同一并发边界；不能先无锁检查，再让旧 apply 插入。

这是防止旧工作污染新生命周期的配套改动，不是声称当前代码已有此保证。实现应封装内部 guard / checked-apply 接口，不暴露可随意使用的互斥锁。

锁顺序需统一为 collection guard → manager 锁 → registry 锁；不得在持有 manager / registry 锁时执行 WAL 或 ETCD I/O。准入阶段的 collection guard 必须在等待 Broadcast ACK 前释放，避免 ACK 内 Put 重新获取同一锁造成死锁。collection DDL 的已有串行化负责请求间顺序，guard 负责它与异步 apply 的交错。

### 9.4 更新、删除与恢复

所有构造 / 转换站点都必须覆盖：expected config、消息生成、现态转 header、Clone、FromMessage、toCollectionProto、buildFromPersisted、store snapshot。

合并规则为“已有 sync 保留；首次显式 sync 建立”，不能让后续省略参数的默认 false 覆盖 true。字段比较应参与幂等判定，但同一生命周期重复请求不能因为重新分配 epoch 而不断广播。

Release 的未完成 view 依然按原状态机恢复、清理。配置持久删除、view durable removal、物理资源释放是不同事件，不制造跨这些系统的虚假原子性。

## 10. QueryView、segment watch 与 QN 复用

### 10.1 view 保存要求，而不是只依赖 watch

仅在 segment watch 中附加 force 标记不足：已加载快捷路径可能不重新执行 watch 驱动的 Load；过期快照也可能晚于新 view 到达。

因此 `QueryViewMeta` 持有创建时的 sync 策略和 epoch。builder 在设置 `LoadInfoVersion` 时，从同一 LoadConfigSnapshot 设置这两个字段。QueryView 保存、恢复、发送、克隆均须保留。

该元数据表达“这个 view 需要什么”。watch payload 表达“这份 segment 元数据按当前策略应怎样加载”。两者不能互相替代。

### 10.2 watch payload 与 revision

在 `buildSnapshots()` 中克隆上游返回的 SegmentLoadInfo，再合并持久 sync 到已有有效 force 要求，随后计算 revision。禁止修改共享快照、返回给其他调用方的对象或 collection schema。

revision 是规范化内容的 hash / 相等性 token，不是可比较新旧的时间序列。不能用数值大小判断哪个 payload 更晚。

需要覆盖初始订阅、新 segment、流断连重订阅、索引 / manifest 更新，以及服务恢复。一次计算使用捕获的不可变配置；不能在循环中不断读取可变全局配置，导致同一批快照内部策略不一致。

### 10.3 执行要求与完成资格

QN 在新建 segment 的执行输入上使用：

```text
effective_force = watch.force_sync_warmup OR view.sync_warmup
```

如果 view 要求 sync，而到达的 watch payload 没带 force，仍必须在任务私有副本上执行 sync，并记录这一不一致；不能直接退回普通加载。该局部执行覆盖不是修改已发布快照的 revision，而是任务显式携带的执行要求。

任务需要绑定：segment identity、view 要求的 epoch、load attempt / state generation，以及使用的 metadata revision。只有对应 Load 成功返回，才能产生该 epoch 的同步预热完成资格。

建议将完成资格表达为一份不可变的运行时记录，至少包含：

- 本次实际执行是否为 force sync。
- `sync_warmup_epoch`。
- 对应的 physical generation / load attempt。
- 最近一次成功应用的 metadata revision。

资格记录“已经完成过这次要求的工作”，不记录“此刻每个 cell 都还热”。eviction 不清除该资格，重复 Load 也不因此触发无限补热。

### 10.4 两个复用入口都必须检查

第一处是 `QueryViewSegmentReadinessManager.activateAcquire()`：当前对 transform-loaded segment 可以直接 report Ready，甚至不进入 physical manager。

第二处是 `ViewScopedPhysicalSegmentManager.collectLoaded()` 及 loaded 通知路径：当前只判断物理 segment 对象存在。

两处都必须调用一致的资格判断：普通 view 保持现有逻辑；sync view 要求 epoch 一致、真实 force 执行完成、generation 对应当前对象。资格应通过共享资源接口访问，不能在两个 manager 内各维护一份可分叉的 true / false。

同一 epoch 内，已完成同步加载的 segment 可以继续供后继 QueryView 复用。不同 epoch，即使旧 segment 曾经 sync，也不能直接算作本轮完成。

### 10.5 残留旧查询与迟到任务

如果旧查询仍持有一个不同 epoch 的 transform segment，第一版选择等待旧资源安全退出，再新建加载，不对旧资源原地补热，也不强行释放旧查询的引用。

实现必须在绑定新 view 引用之前识别这种不兼容：不能先给旧 state 增加新引用，再等待它的引用归零。可重试的 acquire 在旧 query ref 释放后被唤醒；等待受 view 取消和调度器生命周期约束。

若不兼容资源仍属于有效旧 view，应报告内部状态 / 调度不一致并阻止新 view Ready，而不是不断提交重复补热任务。

所有 Load / Reopen 完成回调都验证 state / attempt identity 和 epoch。当前只按 segment ID 找到 state 的模式需要补充校验：一个被取消的旧 Load，即使后来成功返回，也只能被清理，不能安装到同 ID 的新状态中。相同要求适用于迟到的 OnReady、watch 更新和失败回调。

## 11. Go → C++ 与 translator 改造

### 11.1 Go 转换链

当前主路径是 physical loader → `LocalSegment.Load()` → csegment Load → C++ unified Load。

需要修改：

1. `ConvertToSegcoreSegmentLoadInfo()`：复制有效 force 到 segcorepb。
2. CreateCSegment 与 Reopen：统一使用能够保留该字段的转换，不在其中重算 collection 策略。
3. `compactSegmentLoadInfoForRuntime()`：手工重建 querypb 时保留 force。
4. C++ `CompactRuntimeInfoForManifest()`：重建 proto 时保留 force。
5. 更新 legacy index 分支的输入保留与回归，防止其采用旧 runtime info 时降级。

不能通过临时写 collection schema 再恢复的方式实现 force：同一个 collection / segment runtime 可以被多个 view 或查询共享。

### 11.2 C++ 统一解析入口

新增只读 accessor 表达 SegmentLoadInfo 的 force 要求，并在实际创建资源前解析策略。

必须检查的构造站点包括：

- `resolve_field_data_warmup_policy()`：force 在系统字段、raw vector 等特殊分支之前生效，但资源范围由原有 load plan 决定。
- column-group resolver：保留普通模式 `sync > async > disable` 的聚合，force 使已选任务为 sync。
- `ConvertFieldIndexInfo()`：同时覆盖专用 `warmup_policy` 和 index load config 中的 warmup；不能只改其中一份。
- text index / JSON stats 的 load-info 转换：将 force 传入，不重新从 schema 得到 disable。
- `BuildJsonKeyStatsIndex()` 及 JsonKeyStats 下游资源：跳过 defer 判断，并把 sync 继续传入其所有需要的 slot。
- `load_field_data_common()`、PK / timestamp 派生 slot 构造：审查显式空 warmup、固定 disable、缺少 OpContext 的调用。

特别注意：Knowhere lazy-load 索引的外层 translator 本来就可能使用 sync 来加载元数据。这不能替代内部索引资源的同步预热。验收必须观测内层读取，而不是只断言外层 `warmup_policy == sync`。

### 11.3 lazy materialization

`TryLoadLazyManifestColumnGroup()`、JSON stats 的 defer 判断都必须使用 force 后的有效策略。已选资源的 effective policy 为 sync 时，不能进入仅构建 deferred facade 的完成路径。

不修改全局 `lazyManifestReaderEnabled`、`lazyJsonStatsEnabled`，不混用两者，也不根据全局开关直接宣布本请求成功。

已经存在的 lazy facade 绑定旧 runtime generation；单纯更新一个 bool 并 Reopen，未必产生需要重建资源的 diff。因此本设计不允许普通 loaded collection 原地升级。

### 11.4 Load / Reopen 的发布边界

初始 Load：相关资源的同步预热返回成功后，才允许统一 Load 成功、physical loader 回调成功。

同一 sync 生命周期内的 Reopen：新资源先在 staged runtime 中加载、同步预热；所有新资源准备成功后再 Publish。原有资源若已满足同 epoch 要求，可以复用，不要求每次 metadata 更新都重新预热所有未改变资源。

Reopen 失败不能发布半完成 generation，也不能提前把 applied revision 改为新值。保持旧 runtime 的可用性应沿用现有 staged / Publish 机制，更新错误则按现有调度处理；不能把旧 view 可查询误报为新更新已完成。

### 11.5 OpContext 与取消

每个可能阻塞的 sync warmup 都需要可用的操作上下文 / 超时。当前部分 PK、JSON stats 和 slot 创建调用没有向下传递 OpContext，需要逐个补齐，而不是只在最外层检查一次 `ctx.Err()`。

审查范围包括 reader 创建、manifest 读取、远程文件下载、cache cell load、嵌套索引 materialization 和等待并发任务的 join。实现必须确认实际链接的 cachinglayer / knowhere 版本具备所需取消能力；仓库安装头文件不是 native E2E 的替代品。

## 12. 超时、资源与执行并发

### 12.1 超时分层

客户端等待超时、QN 加载任务取消、底层单次预热超时是三种不同事件。

- 客户端停止等待不等于已持久化的 load config 被删除。
- view 被释放、任务被替换或节点关闭时，QN 取消执行；不得继续发送成功回调。
- 底层超时应通过原错误链返回，不降级为 async / disable 后继续报告 loaded。

现有 `loadingTimeoutMs` 与 `warmupLoadingTimeoutMs` 可作为执行预算入口；具体是否覆盖所有新路径需要验证。第一版不承诺仅延长 SDK timeout 就能覆盖底层预算。

### 12.2 内存、磁盘与并发

同步预热会增加 load 延迟、远程 I/O、cache 占用和峰值内存。现有 resource estimator / reservation 必须基于有效 force 后的执行计划估算；不能用普通 lazy 加载的 metadata 估算批准全量并发下载。

沿用 node scheduler 和现有资源限制，不按 collection 的所有 segment 无界展开 warmup。嵌套执行避免占满同一线程池后再同步等待该线程池的子任务。

内存 / 磁盘压力下允许等待、按既有资源规则拒绝或失败，但不允许策略降级。超大目标集不保证能同时驻留；本接口保证要求的预热任务完成，不保证所有预热结果在同一时刻仍未被淘汰。

## 13. 错误处理与失败路径

新增校验遵守现有 merr 分类和包装规范，不在本文预分配数字错误码。Go 增加上下文使用 `merr.Wrap / Wrapf`；C++ 保留有类别的错误，不将其字符串化后重新抛成无类别异常。

| 场景 | 期望分类 / 处理 | Ready 与用户可见结果 |
| --- | --- | --- |
| 不支持的 warmup 值、sync + refresh | 请求参数错误 | 接受 load config 前返回 |
| 普通已加载 collection 申请 sync | 明确的请求 / 状态冲突 | 要求 Release 后重载，不在后台偷偷开启 |
| external collection | 明确不支持 | 接受前拒绝 |
| 上一轮 view 尚未 durable removal | 可重试的系统 not-ready | 不建立错误的完成状态；客户端有界重试 |
| epoch / generation 不匹配 | 隔离旧结果；等待旧引用或报告内部不一致 | 绝不复用成新 Ready |
| S3 限流、网络暂时失败 | 保留系统 / 暂态类别 | 本次加载未完成，是否重试取决于实际调度路径 |
| 文件缺失、损坏、反序列化失败 | 保留原始具体类别；通常不是用户 Load 参数错误 | 加载失败，不能跳过该资源成功 |
| 内存 / 磁盘资源不足 | 现有资源不足类别 | 等待 / 失败，不能切回 disable |
| cancel / deadline | 保留取消 / 超时 | 清理新建资源，不完成旧或新任务 |
| Reopen 新资源预热失败 | 保留错误，不 Publish 新 generation | 旧 runtime 可继续服务；新目标未完成 |
| 混合版本不能提供该能力 | 不支持 / 能力不足 | 不能接受后无声忽略 |

当前初始 segment task 的错误会进入 `OnUnrecoverable`；更新 task 在非取消错误时可能返回 `ErrDelay`。因此“保留了一个 retriable 错误码”不等于“整个加载自动按期望重试”。实施必须分别追踪初始加载的 view 替换 / 重试，以及更新路径的延迟重试和取消。

本需求的硬性保证是：失败不会被计为同步预热完成。对于已有公共加载接口无法立即投递最终失败原因的情况，应保留既有加载状态 / 失败缓存与超时行为，并给出可诊断错误；不能凭空宣称异步服务会把每个底层错误立刻返回最初的 RPC。

## 14. 版本兼容与发布

### 14.1 结构兼容不等于行为兼容

新增 proto 字段的旧值为 false / 0，普通加载可保持兼容。但旧 Proxy 会忽略未知 map 参数，旧 QN / segcore 也可能忽略新增字段，所以不能仅因为 protobuf 能解码就宣称 force sync 支持混部。

第一版采用完整链路能力门禁：Proxy、QueryCoord、相关 WAL 消费者、QueryNode 及其实际 native build 均需支持本设计。部署升级期间不开放显式参数；开启前完成全链路升级。

QueryCoord 调度必须排除不支持 sync load 的节点，或拒绝无法满足 replica 约束的请求。可以复用真实存在的版本 / 能力机制；若无法准确识别能力，必须 fail closed，而不是默认所有在线节点支持。不得用客户端接收了 `warmup` 关键字作为服务端能力证明。

首轮实现使用 `queryCoord.enableLoadCollectionSyncWarmup=false` 作为发布门禁。开启需完成全链路升级与本文验收；该配置不是自动完成升级检测。QN session 新增 `SyncLoadWarmup` 能力声明，由 Go QN 调用实际链接的 native `SupportsSyncLoadWarmup()` 后写入。QC 接受 sync Load 前检查所有可发现、未停止的 QN，未知能力或无可用节点均拒绝；调度分配也过滤不支持的节点。旧 Proxy / WAL 消费者是否完成升级仍由发布流程确认，不能仅靠 QN 的声明证明整条链路支持。

### 14.2 恢复和滚动变更

- 新代码读取旧配置：按普通加载处理。
- 新代码恢复 sync 配置：保留原 epoch，新建资源仍同步预热。
- 新节点接管 sync collection：只有能力满足后才能接收相关 view。
- 旧节点加入：不能被调度到 sync collection。
- 回滚到不理解持久策略的版本前：先释放相关 collection 并完成受控清理，或明确禁止回滚。

不以回滚脚本直接清除 ETCD 中的 sync 字段作为正常关闭方式；这会让持久要求和正在运行的 view 分叉。

## 15. 可观测性

复用现有 segment load timing、view 状态和 load failure 观测框架，增加能解释新阶段的结构化字段：请求是否要求 sync、epoch、资源种类、任务阶段、耗时、结果类别。

epoch、collection ID、segment ID、view key 适合日志 / trace，不作为新增高基数 Prometheus label。日志使用 `mlog` 和真实操作 ctx，避免每个 cell 的高频成功日志。

若需新增指标，建议限定为低基数的预热时延、失败数、等待任务数；label 仅包含资源类别 / 结果类别等有限枚举。指标名称和注册位置沿用本项目既有规范，在实现时确定。

排查至少能回答：

1. 请求的策略、持久策略和当前 view 的 epoch 是否一致？
2. 当前阻塞在资源预约、下载、materialize、sync wait、旧 query ref，还是 transform catchup？
3. 失败发生于哪类资源，是否取消 / 超时，是否发生 view 重建？
4. 是否丢弃过旧 epoch 的回调或复用请求？

缓存命中率不作为 Ready 的输入，也不作为测试唯一依据。cache-cell 命中、字节命中和物理远程 I/O 不是同一指标。

## 16. 实施拆分与文件清单

以下为实施拆分。P0-A / B / C 的首轮代码已写入工作区，P0-D 的 native / E2E 验收尚未完成，具体证据见附录 B。跨步骤可以分 PR，但对外功能只能在完整链路及门禁验收后开放。

### P0-A：协议、公开入口与持久配置

- 公开入口：`internal/proxy/task.go`、Go SDK maintenance options、PyMilvus Prepare / handler、REST V2 load request / handler。
- 协议：query_coord.proto、messages.proto、view.proto、segcore.proto。
- DDL：LoadCollection 的 expected config、现态比较、消息构造、ACK 更新。
- LoadConfig：Clone、持久化、恢复、幂等合并、epoch 分配。
- 保持旧临时 ForceSyncWarmup 和长期 sync 策略独立。

### P0-B：生命周期和 QueryView 不变量

- Balancer builder 从同一快照绑定策略 / epoch。
- LoadConfigStore 与 balancer apply 的 checked-apply / collection fence。
- Registry / manager 的准确排空查询，不依赖 active stats。
- QN 两级复用入口、旧 query ref 等待和迟到回调隔离。
- shared resource 暴露统一完成资格；不重复保存互相矛盾的状态。
- QueryCoord sync 进度使用完整目标 shard 集合，并检查 Up view epoch；补齐 stats 投影和恢复初始化门禁。

### P0-C：实际预热执行闭环

- segment watch 注入 force 后再 hash。
- Go 到 segcore 转换、两侧 runtime compact、Load / Reopen。
- 字段、索引、JSON stats、text、系统派生 slot 的 force 解析。
- OpContext、资源估算、同步错误返回、staged runtime 发布。

### P0-D：失败路径与发布门禁

- 完成第 17 节的故障注入、取消、旧资源复用、恢复测试。
- 证明所有支持节点及 native dependency 实际执行该契约。
- 公共参数文档明确异步 RPC、同步等待、eviction 和加载范围边界。

### 后续可独立讨论

原地升级普通 loaded collection、按字段选择预热、重新预热已被 eviction 的资源、预热进度 API、external collection 支持。它们都需要新的语义，不能靠当前 `warmup="sync"` 隐式扩展。

## 17. 测试设计与验收标准

### 17.1 参数、状态和协议单测

| 测试 | 核心断言 |
| --- | --- |
| 参数未提供 | 编码和行为与当前版本一致 |
| 参数 sync / 非法值 / 空串 | 正确传递；非法输入不进入配置更新 |
| refresh 冲突 | 仅显式 sync + refresh 拒绝；普通 refresh 保留已存在策略 |
| 普通已加载 → sync | 返回清晰冲突，不部分更新配置 |
| 重复 sync / 省略参数 | 策略、epoch 不变，不重复分配 ID / 广播 |
| Clone / Message / ETCD round-trip | bool 和 epoch 在每一处完整保留 |
| QC 重启 | 同一加载生命周期 epoch 不变 |
| Release / Reload | 新 epoch 不等于旧 epoch |
| Namespace Prewarm 完成 | 清理临时字段不清理长期 sync |
| watch payload | force 在 hash 前注入；上游对象和普通 collection 不变 |
| segcore Convert / runtime compact | 初始 Load、Reopen、压缩后 force 均保留 |

### 17.2 调度和复用对抗测试

| 测试 | 核心断言 |
| --- | --- |
| 旧 balancer prepare 迟到 | Release / Reload 后不能安装旧配置的 view |
| 旧 balancer release 迟到 | 不能删除新生命周期的 view |
| Dropping / pending durable removal | stats 已空仍不能错误通过排空检查 |
| 旧 queryRefs 非零 | 新 epoch 不直接 report Ready；不破坏旧查询，不给旧 state 新增阻碍清理的引用 |
| transform-loaded 快捷路径 | 与 physical-loaded 快捷路径使用同一完成资格规则 |
| 同 epoch 的合格 segment | 可以复用，不因同一声明重复预热 |
| 不同 epoch 的旧 sync segment | 不能将上一轮完成资格用于新一轮 |
| 老 Load 取消后迟到成功 | 结果被释放，不装入新 state，不触发新 Ready |
| 老更新 / 老失败回调迟到 | 不修改新 attempt 的 applied revision 或失败状态 |
| view sync + watch force=false | 实际执行仍 force；记录不一致，不静默降级 |
| 目标 shard 尚未全部创建 | registry 已有 shard 全部 Up 也不能提前返回 100% |
| Up view 属于旧 epoch / 目标尚未恢复 | 不计为当前加载完成 |
| 空数据、多 shard、多 replica | 按完整目标统计；所有必要空 view 准备完成后才为 100% |
| 开始等待后 Release | 取消等待 / 加载，回调恰当收敛，无泄漏、死锁 |

这些测试需要明确控制线程 / 回调交错，不能只依赖重复跑成功路径碰概率。

### 17.3 Native 资源覆盖

在全局 warmup 均为 disable 的环境中，分别启用 / 关闭两个 lazy 开关，并验证下列资源。每项都需验证真实数据加载事件和返回顺序，不仅检查参数值。

| 资源 / 配置 | 核心断言 |
| --- | --- |
| V3 manifest 普通字段 | sync 不进入 deferred-only 完成路径，cell 读取完成后 Load 返回 |
| V3 lazy 开关关闭 | force 仍预热；证明 lazy 与 warmup 独立 |
| V1 / V2 字段 | 统一路径有效，不仅 legacy helper 有效 |
| raw vector 无可用索引内部用例 | 特殊策略分支不漏 force；公开加载的索引限制保持不变 |
| 向量索引含内部 lazy load | 内层资源完成，而不是只完成外层 metadata |
| 标量索引 / text-match | 独立 translator 同步加载完整 |
| JSON stats | materialize 及其下游共享键、shredding、column group 完成 |
| PK / timestamp 派生 slot | 实际需要的派生结构接受 force 和取消 |
| 未选字段 / 冗余 raw data | 不因 force 被额外加载 |
| mmap 开启 / 关闭 | 两种模式契约成立；不额外断言 OS 全页驻留 |
| index / manifest 更新 Reopen | 新资源完成后 Publish；失败保留旧 generation |
| 两个 collection 并发加载 | sync collection 不改变普通 collection 策略 |

### 17.4 故障注入与端到端

每个注入点都追踪原始错误 → C++ / 依赖库 → CStatus → Go → QN task → view 状态 → loading progress / SDK 等待。不能只验证最后一层的错误转换函数。

至少覆盖 S3 限流、下载中断、缺失 / 损坏文件、metadata 解析失败、内存不足、磁盘满、操作超时、Release 取消、节点退出和 Reopen 失败。

端到端关键用例：

1. 禁用全局 warmup，开启 lazy manifest / JSON，准备包含向量、标量、text / JSON stats 的可支持数据集。
2. 清理测试环境中的目标缓存，阻塞一个必要资源的真实 load；提交 force sync。
3. 允许 LoadCollection 提交 RPC 返回，但断言相应加载进度不能完成、SDK 同步等待不能成功。
4. 放开资源读取；确认同步预热完成，再观察 physical loaded、Ready 和等待成功。
5. 在新 segment、迁移、新 QN、QC 恢复和索引更新后重复这个 barrier 验证。
6. 故障时断言不出现假 100%；取消后断言资源和回调收敛。
7. Release 后保留旧查询引用并立即 Reload，验证新 epoch 不复用旧 Ready；旧引用释放后新加载可继续。
8. 开启 eviction 验证其行为保持不变；不把后续远程读取视作已违反“本轮预热完成”的契约。

并记录 load 时延、峰值 RSS、磁盘使用、对象存储请求 / 字节、调度队列与取消耗时，评估资源估算是否足够。性能目标需基于明确数据集制定，不在设计中虚构数值。

### 17.5 实施时的验证命令与声明边界

Go 测试使用本仓库要求的 tags 和 gcflags，例如：

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querycoordv2/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querynodev2/qnview/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/views/...
```

这些命令是完整验收入口；本地实际执行结果见附录 B。native 单测与真实依赖构建不可由 mock Go 测试替代。

proto 使用生成流程更新；segcore 的 C++ 生成产物亦通过相应构建流程生成，不手工补字段。若实现触及 wire error projection / oldCode / metric label，按仓库要求补 merr guard 和完整 `make test-go`。提交远端前执行 `make lint-fix` 与 `run_clang_format.sh`。

最终验收必须同时满足：有效输入真的到达每个资源构造点；每种真实失败没有被中途重分类 / 吞掉；Ready barrier 在正常、复用和异常路径都成立。不能以“编译通过 + 成功路径单测通过”代替这些行为证明。

## 18. 备选方案与取舍

| 方案 | 不作为首选的原因 |
| --- | --- |
| 修改全局 warmup 为 sync | 影响其他 collection，不能表达请求级选择 |
| 临时改 collection / field properties | 共享状态有竞态，可能持久污染 schema，恢复与并发语义复杂 |
| 只在 QueryCoord 设置 ForceSyncWarmup | 统一 Go → C++ 转换目前丢失标记，且复用路径仍可绕过 |
| 只在 C++ resolver 增加 force | 缺少请求、持久化、恢复、view 和 Ready 语义 |
| loaded 后再次调用 CacheSlot.Warmup | 现有 slot 可能已执行过一次 Warmup，不保证重复调用有效 |
| 直接复用当前 segment Prewarm() | 当前枚举不覆盖这里要求的所有 text / JSON stats / 派生资源，不能作为完整契约 |
| 只等 Release RPC 返回 | 旧 view、in-flight load 和旧 query ref 不一定已经消失 |
| 只检查协调端排空，不引入 epoch | QN 仍可能保留旧 transform-loaded 对象，无法区分不同轮次的完成资格 |
| 把每次 Load(sync) 都当作重新预热 | 破坏声明式加载幂等，长时间缓存淘汰会使完成语义不稳定 |
| 支持原地升级普通已加载 collection | 需要 generation-aware 全资源补热、增量重建与进度 API，超出第一版 |

本方案相较于“增加一个 bool”多出生命周期标识和复用检查，原因是这些路径在当前代码中真实存在，并直接决定 API 能否诚实地报告完成。

## 19. 评审时应确认的边界

本提案已选择明确行为，以下是实现评审必须证明的事项，不是推迟定义接口：

- checked-apply 与 DDL / load-config store 的锁顺序可实现且无 ACK 死锁。
- QN 在旧 query ref 等待期间不新增错误引用，能取消、能被释放事件唤醒。
- 各 translator 及实际依赖版本的 sync 语义覆盖了本文列出的资源；发现不支持的类型时，应在开放功能前补齐或明确拒绝，不能静默忽略。
- 混合版本的能力门禁真实存在；没有门禁不得宣称支持滚动启用。
- 初始加载和更新的错误路径分别有端到端证据，未验证部分明确列为未完成。

## 附录 A：源码索引

以下为源码基线及本次工作区的实现入口。带行号的基线链接在后续修改后可能有偏移，具体行为以当前文件内容为准。

| 主题 | 源码 |
| --- | --- |
| 配置 | [milvus.yaml](/Users/sunbingyi-zilliz/milvus/configs/milvus.yaml:610)、[paramtable](/Users/sunbingyi-zilliz/milvus/pkg/util/paramtable/component_param.go:4254) |
| Proxy LoadCollection | [task.go](/Users/sunbingyi-zilliz/milvus/internal/proxy/task.go:3168) |
| Go SDK 等待 | [maintenance.go](/Users/sunbingyi-zilliz/milvus/client/milvusclient/maintenance.go:39)、[options](/Users/sunbingyi-zilliz/milvus/client/milvusclient/maintenance_options.go:25) |
| PyMilvus 编码与等待 | [prepare.py](/Users/sunbingyi-zilliz/pymilvus/pymilvus/client/prepare.py:2083)、[grpc_handler.py](/Users/sunbingyi-zilliz/pymilvus/pymilvus/client/grpc_handler.py:1865) |
| REST V2 | [handler_v2.go](/Users/sunbingyi-zilliz/milvus/internal/distributed/proxy/httpserver/handler_v2.go:1012) |
| 内部协议 | [query_coord.proto](/Users/sunbingyi-zilliz/milvus/pkg/proto/query_coord.proto:249)、[messages.proto](/Users/sunbingyi-zilliz/milvus/pkg/proto/messages.proto)、[view.proto](/Users/sunbingyi-zilliz/milvus/pkg/proto/view.proto:107)、[segcore.proto](/Users/sunbingyi-zilliz/milvus/pkg/proto/segcore.proto:117) |
| QC DDL | [load collection callback](/Users/sunbingyi-zilliz/milvus/internal/querycoordv2/ddl_callbacks_alter_load_info_load_collection.go:42)、[ACK callback](/Users/sunbingyi-zilliz/milvus/internal/querycoordv2/ddl_callbacks_alter_load_info.go:27)、[services](/Users/sunbingyi-zilliz/milvus/internal/querycoordv2/services.go:270) |
| 公开加载进度 | [Proxy GetLoadingProgress](/Users/sunbingyi-zilliz/milvus/internal/proxy/impl.go:2204)、[getCollectionProgress](/Users/sunbingyi-zilliz/milvus/internal/proxy/util.go:2558)、[qviewsLoadPercentage](/Users/sunbingyi-zilliz/milvus/internal/querycoordv2/services.go:200)、[ShardStats](/Users/sunbingyi-zilliz/milvus/internal/views/coord/coordview/shard_stats.go) |
| 持久化 | [load_config.go](/Users/sunbingyi-zilliz/milvus/internal/views/coord/loadmgr/load_config.go)、[load_config_store.go](/Users/sunbingyi-zilliz/milvus/internal/views/coord/loadmgr/load_config_store.go) |
| 调度与 view 构造 | [balancer.go](/Users/sunbingyi-zilliz/milvus/internal/views/coord/balancer/balancer.go:234)、[allocate.go](/Users/sunbingyi-zilliz/milvus/internal/views/coord/balancer/allocate.go:101)、[builder.go](/Users/sunbingyi-zilliz/milvus/internal/views/qviews/builder.go:64) |
| durable removal | [shard_view_manager.go](/Users/sunbingyi-zilliz/milvus/internal/views/coord/coordview/shard_view_manager.go:735)、[shard_view_registry.go](/Users/sunbingyi-zilliz/milvus/internal/views/coord/coordview/shard_view_registry.go:176) |
| segment watch | [segment_load_info_watch.go](/Users/sunbingyi-zilliz/milvus/internal/querycoordv2/segment_load_info_watch.go:191) |
| QN 主加载 | [physical loader](/Users/sunbingyi-zilliz/milvus/internal/querynodev2/qvresource/qv_physical_segment_loader.go)、[segment_loader.go](/Users/sunbingyi-zilliz/milvus/internal/querynodev2/segments/segment_loader.go:1185)、[segment.go](/Users/sunbingyi-zilliz/milvus/internal/querynodev2/segments/segment.go:1357) |
| QN 任务与复用 | [segment_scheduler_task.go](/Users/sunbingyi-zilliz/milvus/internal/querynodev2/qnview/segment_scheduler_task.go)、[physical manager](/Users/sunbingyi-zilliz/milvus/internal/querynodev2/qnview/view_scoped_physical_segment_manager.go)、[readiness manager](/Users/sunbingyi-zilliz/milvus/internal/querynodev2/qnview/queryview_segment_readiness_manager.go) |
| Go → C++ | [segcore segment.go](/Users/sunbingyi-zilliz/milvus/internal/util/segcore/segment.go:431) |
| C++ 统一 Load / Reopen | [ChunkedSegmentSealedImpl.cpp](/Users/sunbingyi-zilliz/milvus/internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp) |
| C++ 转换 / compact | [SegmentLoadInfo.cpp](/Users/sunbingyi-zilliz/milvus/internal/core/src/segcore/SegmentLoadInfo.cpp)、[SegmentLoadInfo.h](/Users/sunbingyi-zilliz/milvus/internal/core/src/segcore/SegmentLoadInfo.h) |
| 策略范围保护 | [Utils.cpp](/Users/sunbingyi-zilliz/milvus/internal/core/src/segcore/Utils.cpp:1396) |
| 向量索引 translator | [SealedIndexTranslator.cpp](/Users/sunbingyi-zilliz/milvus/internal/core/src/segcore/storagev1translator/SealedIndexTranslator.cpp) |
| JSON stats | [JsonKeyStats.cpp](/Users/sunbingyi-zilliz/milvus/internal/core/src/index/json_stats/JsonKeyStats.cpp) |
| 本地安装的缓存依赖头文件 | [Manager.h](/Users/sunbingyi-zilliz/milvus/internal/core/output/include/cachinglayer/Manager.h)、[CacheSlot.h](/Users/sunbingyi-zilliz/milvus/internal/core/output/include/cachinglayer/CacheSlot.h) |

## 附录 B：实施与验证记录（2026-09-07）

### B.1 已写入工作区的首轮代码

- 入口：Go SDK `WithSyncWarmup()`、PyMilvus `warmup="sync"`、REST V2 load 参数透传；Proxy / QC 拒绝不支持的值、显式 sync + refresh 和 external collection。REST refresh 也透传显式参数供 Proxy 拒绝，不静默丢弃。
- 持久化：公开 map → 内部请求 → WAL header → LoadConfig / CollectionLoadInfo，增加独立 `SyncWarmup` 和持久 epoch。其他 DDL 省略字段时保留策略；普通已加载 collection 不能原地开启；Release 才结束该生命周期。恢复时拒绝不完整 epoch。
- 生命周期：LoadConfigStore 的 per-collection checked-apply、旧 view durable removal 检查、QN physical / transform-loaded 两级资格校验。只等待残留旧 query ref；不兼容资源仍属于有效旧 view 时明确失败。迟到 Load 校验 state / attempt；catch-up、失败和 reset 按原 view / segment 实例校验，不能仅凭 segment ID 影响替代对象。资格使用 immutable segment decorator，不表示 cache residency lease；decorator 保留原 segment 的查询接口与 transform 起点。
- 进度：[sync_warmup_progress.go](/Users/sunbingyi-zilliz/milvus/internal/querycoordv2/sync_warmup_progress.go) 从 collection metadata 确定完整 vchannel 集合，按 vchannel × replica 计数，只统计匹配 epoch 的 sync Up。metadata 缓存写入与 Release / reload 用配置版本隔离。
- 执行：[sync_warmup.go](/Users/sunbingyi-zilliz/milvus/internal/querynodev2/qnview/sync_warmup.go) 生成私有 force load-info；watch 注入 force 后计算 revision。Go → segcore 及两侧 compact 保留 force；C++ 字段、索引双策略、text / JSON stats、PK / timestamp 派生 slot 覆盖为 sync。为新增同步 slot 路径传递 OpContext；Load / Update 取消后不发送成功回调。
- 发布：增加默认关闭的 QC 门禁和 QN/native 能力声明，调度器检查节点能力和 Up epoch。没有修改全局 warmup / lazy / mmap，也没有实现原地开启。
- 协议生成：通过 `make -o download-milvus-proto generated-proto-without-cpp` 更新。仅跳过已经核对 revision 的第三方 proto 下载步骤，避免下载脚本重置依赖 checkout；生成文件未手工编辑。

### B.2 已执行并通过

- `internal/views/coord/loadmgr`、`internal/views/coord/balancer`、`internal/views/coord/coordview`、`internal/views/qviews/...` 的完整包单测，使用 `-tags dynamic,test -gcflags="all=-N -l" -count=1`。包括持久化 / 恢复校验、禁止原地开启、sticky / Release、配置版本 fence、能力与 epoch 分类、durable removal 检查。
- Go SDK `TestLoadCollectionSyncWarmupOption`；PyMilvus `tests/unit/test_load_warmup.py` 的 10 个用例。
- QN session 能力 JSON round-trip 定向单测；隔离本地配置的 `TestLoadCollectionSyncWarmupGateDefaultsClosed`。
- QueryCoord 的 sync 策略解析、缺失 / 未知节点能力拒绝、完整 shard × replica 进度的定向单测；调度器同步验证无合格节点时不分配、只分配给合格节点、view 携带同一配置快照的 epoch。
- 相关 Go proto 包构建（这些包本身没有测试用例）。
- C++ `SegmentLoadInfo.cpp`、`JsonKeyStats.cpp`、`segment_c.cpp` 对象编译；`ChunkedSegmentSealedImpl.cpp` 与包含新增 converter / compact 用例的 `test_loading.cpp` 在关闭 OpenMP / PCH 的诊断命令下通过 syntax-only 检查。后两项不是正式构建或 native 单测通过。
- 所有本次涉及的 root Go 包、`client/milvusclient` 和 `pkg/util/paramtable` 按 `golangci-lint --new-from-rev=HEAD` 检查均为 0 issues；不是全仓存量问题清零。PyMilvus 修改的源文件通过 Ruff 检查，源文件和新测试通过格式检查。
- Go 改动格式化、改动行 clang-format 检查及 `git diff --check`。没有运行提交远端前的全仓格式化流程，也没有提交 / 推送。

其中依赖 native 的 Go 纯逻辑单测使用当前源码 C 头文件及本地库 rpath。它们验证了调度 / 元数据逻辑，不代表旧安装库具备新的实际预热能力。

### B.3 尚未通过的发布验收

- QN / Proxy / REST 的新增测试已编写；执行仍受安装库版本阻塞。默认 include 下出现缺少声明和 FFI 参数数不一致；改用当前源码头文件后，相关测试在链接阶段缺少当前源码所需 native 符号，包括新能力接口，不能执行。不能将“已编写用例”记为“测试通过”。
- QueryCoord 全包执行失败于 `TestServer/TestStop` 和 `TestServer/TestUpdateAutoBalanceConfigLoop`。独立复跑分别定位到：重复 `Server.Stop → streaming manager.Close` 产生 `close of closed channel`；`ServerSuite.SetupTest → MockQueryNode.Start → Session.Register` 发生 etcd session CAS 冲突。这些失败不在新增 warmup 路径内；不能把新增定向用例通过等同于全包回归通过。未修改无关的 Stop / session 注册逻辑来绕过失败。
- 正式 `milvus_core` 构建遇到当前 Knowhere 与本地 LLVM 18 / libc++ 的 `std::atomic_ref` 兼容问题；单独编译 ChunkedSegmentSealedImpl 还遇到既有 structured-binding lambda capture 与 OpenMP 的编译器限制。未为本功能修改第三方依赖或全局工具链来绕过它们。
- `pkg/util/paramtable` 全包测试受本地配置覆盖影响存在默认值断言失败；独立的新门禁用例通过，不宣称全包通过。
- 未运行第 17 节要求的真实 Storage V1 / V2 / V3、内层 Knowhere、text / JSON stats I/O barrier、S3 限流 / 损坏文件 / OOM / cancel 故障注入，以及恢复、迁移、并发 Release/reload 的 E2E。
- cache slot 已传递 OpContext；manifest / JSON metadata 和底层文件读取能否及时中断，仍需以真实依赖实测。初始 Load 错误进入既有失败回调、Update 错误继续既有延迟重试，不额外承诺 SDK 立即收到所有异步错误。
- 资源估算保留现有分层：tiered eviction 下缓存资源由 caching layer 预留，Go 估算非缓存部分；关闭 eviction 时估算缓存与非缓存部分。私有 force 输入在 reservation 前生成，但峰值、超时和嵌套线程池行为尚未完成压力验收，不新增“全量驻留”保证。

在上述 native / E2E 项目完成前，维持门禁关闭，当前补丁按“实施中、待验收”交付。
