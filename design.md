# 基于 IBKR API 的个人量化交易平台设计

> 本文是架构基线和演进目标，不保证每个示例都与当前 CLI 一字不差。当前操作方式以
> [README.md](README.md) 为准，策略实现以 [STRATEGIES.md](STRATEGIES.md) 为准，
> 已交付阶段和技术债以 [stages.md](stages.md) 为准。

## 0. 当前实现与设计基线的差异

当前代码已经是由后端、`rpc-types` 和 Yew `web` 组成的 Cargo workspace。
`jsonrpsee` 在配置指定的 TCP 地址上提供 HTTP/WebSocket JSON-RPC；示例配置使用
loopback，但监听并非硬编码，外部监听需要额外的 TLS、认证和网络访问控制。

策略层现有五种注册实现：`moving_average_cross`、
`moving_average_cross_5s`、`moving_average_cross_v2`、`close_threshold` 和
`paper_round_trip`。实时与回测共享同步、确定性的 `Strategy::evaluate` 核心；
自动执行仅限 paper，采用目标仓位语义，可配置空头和多腿，但当前策略订单只使用
常规时段市价单；启用盘前盘后后改用最新 Bid/Ask 定价的限价单。

策略代码采用静态编译的三层 crate 结构：`model` 共享配置类型、字段 schema 和能力
元数据，`engine` 提供后端算法与 factory，`web` 提供 Yew 展示/表单组件。后端和
Web 各自通过 Catalog 注册策略，因此平台运行器不再按策略 `kind` 维护 factory
分支，策略参数展示也不再写在通用页面中。Broker、存储、风控、成本与订单执行仍由
平台掌握，策略 crate 无法绕过这些边界。

DuckDB 当前由进程内互斥保护的 `Storage` 统一访问，而不是下文概念图中的独立
Storage Writer actor。数据库最新 schema 为 25：除账户、行情、策略、订单、成交、
风控和绩效数据外，还包含 5 秒 Bar、订单剩余数量/最近成交价/`why_held`/
market-cap price、`broker_order_events` 状态事件审计，以及支持正常/扩展和单日多
区间的 IBKR 交易日历缓存。

当前回测是多头、下一根 Bar 开盘撮合，佣金参数是每笔固定金额；它不会自动计算
不同市场的阶梯佣金、最低收费、税费或平台费。实时绩效则使用 IBKR 实际
`CommissionReport`。实时自动执行可绑定数据库费用模型，以每笔固定费、每股费、
比例费、最低费、税费、点差、滑点和实际佣金 P90 建立成本门槛，并在佣金/毛利润
超限时自动停用。

## 1. 文档目的

本文描述一个面向个人使用、长期后台运行的量化交易平台。平台通过 Interactive Brokers（IBKR）TWS 或 IB Gateway 连接市场和账户，完成：

- 合约发现与管理；
- 实时行情订阅；
- 历史行情采集与补齐；
- 策略计算、信号生成和风险检查；
- 订单提交、撤单及生命周期跟踪；
- 账户、持仓、成交和盈亏同步；
- 基于 DuckDB 与 Parquet 的研究和批量分析；
- 通过 JSON-RPC 控制后台进程；
- 使用同一程序提供命令行客户端。

这是个人平台，不以多租户、高频交易或跨地域高可用为目标。设计优先级依次为：资金安全、状态可恢复、数据正确、可观测、实现简单、扩展能力。

## 2. 技术约束与选型

| 领域 | 选型 | 说明 |
|---|---|---|
| 语言与异步运行时 | Rust + Tokio | 业务编排、并发任务、RPC 和定时任务使用 Tokio |
| IBKR 客户端 | `ibapi` crate | 与 TWS/IB Gateway 通信；阻塞接口与 Tokio 隔离 |
| 嵌入式分析数据库 | DuckDB | 元数据、当前状态、查询视图和批量 SQL 分析 |
| 历史文件 | Parquet | 不可变或追加式历史数据文件 |
| 内存数据 | 普通 Rust struct | 初期不在核心业务中引入 Arrow；在存储边界做转换 |
| 批量分析 | DuckDB SQL | SQL 不足时再引入 Polars，不作为首版核心依赖 |
| 时间 | `chrono` | 内部使用 UTC；数据库和 Parquet 均统一保存 UTC |
| ID | UUID / bigint + IBKR conid | 内部实体不依赖外部 ID；合约同时保存 `conid` |
| 日志 | `tracing` | 结构化日志、span、轮转文件和可选 JSON 输出 |
| 配置 | TOML + `serde` | 文件配置、环境变量覆盖、启动时校验 |
| RPC | JSON-RPC 2.0 | `jsonrpsee`，监听地址可配置，CLI 使用 HTTP、Web 使用 WebSocket |
| CLI | 同一二进制 | `clap` 子命令通过 JSON-RPC 操作 daemon |

建议使用当前稳定 Rust 工具链。依赖版本在实现时锁定到 `Cargo.lock`，升级必须通过回归测试，尤其是 `ibapi`、DuckDB 和 Parquet 写入相关依赖。

## 3. 范围与非目标

### 3.1 首版范围

- 单用户、单台机器；
- 可配置一个或多个 IBKR 会话，但首版只启用一个交易会话；
- 股票、ETF 的基础行情和订单；数据模型为后续期权、期货和外汇预留字段；
- 日线和分钟 K 线历史数据；
- 实时 tick 或 bar 订阅；
- 市价单、限价单、止损单及基础组合订单；
- 回测、模拟运行和实盘运行采用相同策略接口；
- RPC 查询、管理和交易操作；
- 可审计的订单意图、风险决策、订单状态和成交记录。

### 3.2 非目标

- 微秒级或超低延迟交易；
- 多租户权限与计费；
- 分布式数据库和分布式一致性；
- 将 DuckDB 当作高并发 OLTP 数据库；
- 完整复刻 IBKR 的所有产品类型和 API 功能；
- 在首版中引入 Arrow 作为贯穿系统的内存表示。

## 4. 总体架构

系统采用“单进程、模块化、消息驱动、关键状态持久化”的结构。

```text
                        +----------------------+
                        | quant CLI            |
                        | status/data/order/...|
                        +----------+-----------+
                                   |
                  JSON-RPC 2.0 / configured TCP
                                   |
+----------------------------------v----------------------------------+
| quant daemon                                                        |
|                                                                     |
|  +-------------+     +----------------+     +-------------------+    |
|  | RPC Server  |---->| Application    |---->| Risk Engine       |    |
|  +-------------+     | Services       |     +---------+---------+    |
|                      +--+----+-----+---+               |              |
|                         |    |     |                   v              |
|                 +-------+    |     +-----------> Order Manager       |
|                 |            |                         |              |
|         +-------v------+ +---v------------+      +-----v----------+  |
|         | Market Data  | | Strategy Host  |      | IBKR Gateway   |  |
|         +-------+------+ +---+------------+      | adapter/actor  |  |
|                 |            |                   +-----+----------+  |
|                 +------> Event Bus <-------------------+             |
|                              |                                      |
|                   +----------+-----------+                          |
|                   | Storage Writer       |                          |
|                   +-------+--------------+                          |
|                           |                                         |
|                   +-------v-------+    +------------------------+    |
|                   | DuckDB        |    | Parquet data lake      |    |
|                   +---------------+    +------------------------+    |
+---------------------------------------------------------------------+
                                   |
                              TWS / IB Gateway
                                   |
                                  IBKR
```

核心原则：

1. `ibapi` 连接由专用适配器拥有，不允许各业务模块直接调用；
2. 交易写操作经过“订单意图 → 风险检查 → 订单管理器 → IBKR 适配器”单一路径；
3. DuckDB 只由受控存储服务写入，避免多个异步任务直接并发写；
4. 大体量时序历史数据写入 Parquet，DuckDB 保存目录、清单和查询视图；
5. 内部事件携带唯一 ID、发生时间和关联 ID，以支持去重、恢复和审计；
6. daemon 是唯一拥有交易连接和可变状态的进程，CLI 不直接访问数据库或 IBKR。

## 5. 推荐项目结构

初期可以使用一个 package，模块稳定后再拆为 workspace：

```text
simple-quantitative-trader/
├── Cargo.toml
├── config/
│   └── example.toml
├── migrations/
│   ├── 0001_init.sql
│   └── ...
├── src/
│   ├── main.rs
│   ├── cli.rs
│   ├── config.rs
│   ├── error.rs
│   ├── ids.rs
│   ├── time.rs
│   ├── domain/
│   │   ├── contract.rs
│   │   ├── market_data.rs
│   │   ├── account.rs
│   │   ├── order.rs
│   │   ├── portfolio.rs
│   │   └── strategy.rs
│   ├── ibkr/
│   │   ├── actor.rs
│   │   ├── mapping.rs
│   │   ├── pacing.rs
│   │   └── recovery.rs
│   ├── application/
│   │   ├── commands.rs
│   │   ├── queries.rs
│   │   └── services.rs
│   ├── market_data/
│   │   ├── realtime.rs
│   │   ├── historical.rs
│   │   └── aggregation.rs
│   ├── execution/
│   │   ├── manager.rs
│   │   ├── state_machine.rs
│   │   └── reconciliation.rs
│   ├── risk/
│   │   ├── engine.rs
│   │   └── rules.rs
│   ├── strategy/
│   │   ├── host.rs
│   │   ├── context.rs
│   │   └── builtins/
│   ├── storage/
│   │   ├── duckdb.rs
│   │   ├── parquet.rs
│   │   ├── catalog.rs
│   │   └── migrations.rs
│   ├── analysis/
│   │   ├── query.rs
│   │   └── backtest.rs
│   ├── rpc/
│   │   ├── server.rs
│   │   ├── types.rs
│   │   └── methods.rs
│   └── telemetry.rs
├── tests/
│   ├── integration/
│   └── fixtures/
└── data/                       # 默认不提交 Git
```

## 6. 领域模型

### 6.1 ID 规则

- 跨表、跨重启、需要对外暴露的内部实体使用 UUID v7，例如 `OrderIntentId`、`RunId` 和 `StrategyId`；
- 数据库内部高频追加记录可使用 `BIGINT` 序列键；
- IBKR 合约保留 `conid: i64`，但不能用其替代内部 `InstrumentId`；
- IBKR `order_id` 只在特定客户端/会话语境中有意义；使用内部 `OrderId` 作为主键；
- 类型安全上使用 newtype，避免混用 ID。

```rust
pub struct InstrumentId(pub uuid::Uuid);
pub struct OrderId(pub uuid::Uuid);
pub struct OrderIntentId(pub uuid::Uuid);
pub struct StrategyId(pub uuid::Uuid);
pub struct IbkrConId(pub i64);
pub struct IbkrOrderId(pub i32);
```

### 6.2 时间规则

- 业务层使用 `chrono::DateTime<Utc>`；
- 只表示交易日时使用 `chrono::NaiveDate`；
- 配置中接受带时区的 RFC 3339 时间，不接受含糊的本地时间；
- DuckDB 使用 `TIMESTAMPTZ` 表示时刻；
- Parquet 使用 UTC adjusted 的 `TIMESTAMP_MICROS`；
- IBKR 返回的交易所本地时间必须结合合约交易所时区解析，并立刻转换为 UTC；
- 保存原始时间文本和解析来源，方便诊断夏令时问题；
- K 线统一定义 `[open_time, close_time)`，唯一键至少包含合约、周期、开盘时间和数据类型。

### 6.3 主要结构体

```rust
pub struct Instrument {
    pub id: InstrumentId,
    pub conid: Option<IbkrConId>,
    pub symbol: String,
    pub local_symbol: Option<String>,
    pub security_type: SecurityType,
    pub currency: String,
    pub exchange: String,
    pub primary_exchange: Option<String>,
    pub timezone: String,
    pub multiplier: Option<rust_decimal::Decimal>,
    pub min_tick: Option<rust_decimal::Decimal>,
}

pub struct Bar {
    pub instrument_id: InstrumentId,
    pub timeframe: Timeframe,
    pub open_time: chrono::DateTime<chrono::Utc>,
    pub close_time: chrono::DateTime<chrono::Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: Option<f64>,
    pub wap: Option<f64>,
    pub trade_count: Option<u64>,
    pub source: DataSource,
    pub is_final: bool,
}

pub struct OrderIntent {
    pub id: OrderIntentId,
    pub strategy_id: Option<StrategyId>,
    pub instrument_id: InstrumentId,
    pub side: Side,
    pub quantity: rust_decimal::Decimal,
    pub order_type: OrderType,
    pub limit_price: Option<rust_decimal::Decimal>,
    pub stop_price: Option<rust_decimal::Decimal>,
    pub time_in_force: TimeInForce,
    pub outside_rth: bool,
    pub idempotency_key: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
```

行情计算可用 `f64`；订单数量、订单价格、现金和盈亏建议使用 `rust_decimal::Decimal`，避免货币运算误差。在发往 IBKR 和写入 Parquet 前执行显式转换和精度校验。

## 7. 组件详细设计

## 7.1 进程入口与生命周期

同一二进制建议命名为 `quant`：

```text
quant daemon [--config PATH]
quant status
quant account summary
quant data backfill ...
quant strategy start ...
quant order submit ...
quant shutdown
```

`daemon` 启动顺序：

1. 解析命令行和配置路径；
2. 加载、覆盖并校验配置；
3. 初始化日志和 panic hook；
4. 获取数据目录中的进程锁，防止第二个 daemon 同时运行；
5. 打开 DuckDB，检查 schema 版本并执行迁移；
6. 检查或恢复 Parquet 临时文件与清单；
7. 启动存储写入器、内部事件总线和 RPC 服务；
8. 启动 IBKR 适配器并建立连接；
9. 执行账户、持仓、开放订单和成交对账；
10. 对账成功后才进入 `Ready`，允许自动交易；
11. 恢复行情订阅、数据任务和配置为自动启动的策略。

优雅停止顺序：

1. 状态切为 `Draining`，拒绝新的策略启动和订单意图；
2. 停止策略并取消后台数据任务；
3. 可配置是否取消 daemon 所有工作订单，默认不自动撤单；
4. 刷新存储队列、完成 Parquet 原子提交；
5. 断开 IBKR；
6. 关闭 RPC，释放文件锁。

使用 `tokio_util::sync::CancellationToken` 传播停止信号，使用 `JoinSet` 管理任务。关键任务异常退出时进入降级或停止状态，不能静默重启并继续交易。

## 7.2 IBKR Gateway 适配器

`ibapi` 的连接、请求和事件读取可能包含阻塞操作。不能在 Tokio worker 线程上直接长时间执行。建议采用“专用线程/阻塞 actor”：

```text
Tokio modules
    |
    | mpsc<IbkrCommand> + oneshot response
    v
IBKR actor on dedicated OS thread
    |
    | ibapi Client
    v
TWS / IB Gateway
    |
    | normalized DomainEvent
    v
Tokio event ingress
```

`IbkrCommand` 示例：

- `ResolveContract`;
- `RequestHistoricalData`;
- `SubscribeMarketData`;
- `CancelMarketData`;
- `PlaceOrder`;
- `CancelOrder`;
- `RequestOpenOrders`;
- `RequestExecutions`;
- `RequestPositions`;
- `RequestAccountSummary`;
- `Disconnect`.

每个命令包含内部 `request_id`、截止时间和必要时的 `oneshot::Sender`。长流请求返回 subscription handle，事件通过有界 `mpsc` 发送。禁止无限队列：

- 订单、成交和错误事件绝不能静默丢弃；
- 高频行情在队列满时可以按合约合并最新值，或明确记录丢弃计数；
- 历史请求采用限速队列，不因 RPC 调用数量直接冲击 IBKR。

连接状态机：

```text
Disconnected -> Connecting -> Synchronizing -> Ready
      ^              |              |           |
      +--------------+--------------+-----------+
                    error/reconnect
```

重连策略：

- 指数退避并加入 jitter，例如 1 秒到 60 秒封顶；
- 区分认证失败、端口错误、TWS 未启动和临时网络错误；
- 重连后重新获取 `nextValidId`，不自行猜测 IBKR 订单 ID；
- 重建行情订阅；
- 查询开放订单、成交、账户和持仓并对账；
- 对账完成前禁止新订单，撤单等降低风险的动作可保留；
- 每个 API 错误映射为结构化内部错误，保存 IBKR error code 和 request/order ID。

客户端 ID 规则：

- 配置固定的 `client_id`；
- 实盘与模拟使用不同的 client ID 和数据目录；
- 不允许两份 daemon 使用相同账户、client ID 和数据目录；
- 是否绑定或接管其他客户端订单必须显式配置，默认只管理本平台订单。

IBKR pacing：

- 所有请求经过按类别划分的 token bucket；
- 历史数据按合约、时间范围和 bar size 切片；
- pacing 参数使用保守默认值并允许配置；
- 对 pacing violation 做延迟重试，不能立即循环重试；
- 请求计划和重试结果持久化，重启后可继续。

## 7.3 内部事件总线

事件总线解耦 IBKR、策略、存储和 RPC 通知。首版使用进程内 Tokio channel，不引入 Kafka/NATS。

事件统一包络：

```rust
pub struct EventEnvelope<T> {
    pub event_id: uuid::Uuid,
    pub event_time: chrono::DateTime<chrono::Utc>,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub correlation_id: Option<uuid::Uuid>,
    pub causation_id: Option<uuid::Uuid>,
    pub source: EventSource,
    pub payload: T,
}
```

事件类别：

- 市场数据事件；
- IBKR 连接和错误事件；
- 账户、持仓、订单、成交事件；
- 风险决策事件；
- 策略生命周期、信号和指标事件；
- 数据任务进度事件；
- 系统健康和配置变更事件。

关键交易事件采用可靠路径：先写审计表或事务性状态表，再向非关键订阅者广播。`broadcast` 只用于 UI/RPC 通知等允许落后的消费者，不能作为订单状态的唯一载体。

## 7.4 合约与证券主数据

用户输入通常是 symbol/exchange/currency，但交易和行情最终绑定到 `conid`。合约解析流程：

1. CLI/RPC 提交合约查询；
2. 调用 IBKR contract details；
3. 若结果不唯一，返回候选列表，禁止自动猜测；
4. 用户确认后创建内部 `Instrument`；
5. 保存完整合约快照与 `conid`；
6. 后续请求优先使用已确认的合约信息。

证券主数据应保留交易所时区、最小价格变动、乘数、到期日、行权价、right、trading class 等扩展字段。IBKR 合约详情变化时创建新快照，而非覆盖所有历史信息。

## 7.5 行情服务

### 实时行情

`MarketDataService` 管理引用计数订阅：多个策略订阅同一数据时只向 IBKR 建立一份上游订阅。最后一个消费者退出后再取消。

处理过程：

1. IBKR 原始事件映射成普通 Rust struct；
2. 校验合约、时间和价格；
3. 补充 `received_at`，保留源时间；
4. 可选地由 tick 聚合成固定周期 bar；
5. 向策略发布；
6. 批量写入内存缓冲，并定期持久化。

未完成 bar 使用 `is_final = false`，结束后以相同业务键输出 final bar。策略必须声明是否接受未完成 bar，默认只使用 final bar。

### 历史行情

历史任务模型：

- `BackfillJob`：目标合约、数据类型、bar size、起止时间；
- `BackfillChunk`：IBKR 实际请求片段；
- `DatasetManifest`：已有覆盖范围、文件和统计信息；
- `DataGap`：缺失、重复、非法或可疑时间段。

下载必须幂等。以业务键去重；写入新文件前先落临时文件，校验后原子 rename，再提交 DuckDB 清单。进程崩溃后可以根据临时文件和清单恢复。

质量检查至少包含：

- `low <= open/close <= high`；
- 时间递增且符合周期边界；
- 无重复业务键；
- 交易时段内缺口检测；
- 价格、成交量非负且无 NaN/Infinity；
- 相邻分片边界无重复或缺失；
- 明确区分 `TRADES`、`MIDPOINT`、`BID`、`ASK` 和是否调整价格。

## 7.6 Parquet 数据湖

建议目录：

```text
data/lake/
├── bars/
│   ├── timeframe=1d/
│   │   └── instrument_id=<uuid>/year=2026/part-<uuid>.parquet
│   └── timeframe=1m/
│       └── instrument_id=<uuid>/date=2026-07-27/part-<uuid>.parquet
├── ticks/
│   └── instrument_id=<uuid>/date=2026-07-27/part-<uuid>.parquet
└── executions/
    └── year=2026/month=07/part-<uuid>.parquet
```

分区原则：

- 不把 symbol 作为唯一分区键，symbol 可能变化；使用内部 `instrument_id`；
- 日线按年分区，分钟和 tick 按日分区；
- 控制小文件，目标文件大小可设为 64–256 MiB；
- 小批数据先写 staging，后台 compaction 合并；
- Parquet 文件视为不可变，修正数据时写新文件并更新 manifest，使旧文件失效；
- schema 包含 `schema_version` 或在 manifest 中关联版本；
- 使用 ZSTD 压缩，行情通常适合适中的压缩级别。

尽管核心内存模型是普通 struct，Parquet writer 边界仍可能使用 Arrow builder 或 Parquet crate 所需列式表示。该转换封装在 `storage::parquet` 内，不向领域层泄漏 Arrow 类型。

每个文件在 `dataset_files` 表登记：

- file ID、数据集和相对路径；
- schema version；
- instrument/timeframe/data type；
- 最小/最大事件时间；
- 行数、文件大小和可选 checksum；
- `active`、创建时间和替代关系。

DuckDB 查询只读取 manifest 中 active 文件，避免通配符把临时文件或过期文件读入。

## 7.7 DuckDB 存储

DuckDB 用于：

- 配置派生的运行元数据；
- instrument、strategy、job 等实体；
- 当前账户、持仓和订单状态；
- 订单/成交/风险审计；
- Parquet 文件 catalog；
- SQL 分析视图和回测结果。

DuckDB 不适合被许多 Tokio task 同时当成事务数据库使用。设计为单写者：

- `StorageWriter` 独占写连接和写命令队列；
- 写命令在 `spawn_blocking` 或专用线程中执行；
- 读查询使用少量独立只读连接，并限制并发；
- RPC 长查询有超时、结果行数和内存限制；
- 写操作按事件批处理，但订单意图等关键记录立即提交；
- daemon 运行期间，禁止 CLI 直接打开数据库。

### 主要表

```sql
CREATE TABLE instruments (
    instrument_id UUID PRIMARY KEY,
    conid BIGINT,
    symbol VARCHAR NOT NULL,
    local_symbol VARCHAR,
    security_type VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    primary_exchange VARCHAR,
    timezone VARCHAR NOT NULL,
    multiplier DECIMAL(38, 12),
    min_tick DECIMAL(38, 12),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX instruments_conid_idx ON instruments(conid);

CREATE TABLE order_intents (
    order_intent_id UUID PRIMARY KEY,
    idempotency_key VARCHAR NOT NULL UNIQUE,
    strategy_id UUID,
    instrument_id UUID NOT NULL,
    payload_json JSON NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    order_intent_id UUID NOT NULL,
    broker VARCHAR NOT NULL,
    broker_order_id BIGINT,
    broker_perm_id BIGINT,
    status VARCHAR NOT NULL,
    filled_quantity DECIMAL(38, 12) NOT NULL,
    average_fill_price DECIMAL(38, 12),
    version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE order_events (
    event_seq BIGINT PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    order_id UUID NOT NULL,
    event_type VARCHAR NOT NULL,
    broker_status VARCHAR,
    payload_json JSON NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE executions (
    execution_id UUID PRIMARY KEY,
    broker_execution_id VARCHAR NOT NULL UNIQUE,
    order_id UUID,
    instrument_id UUID NOT NULL,
    side VARCHAR NOT NULL,
    quantity DECIMAL(38, 12) NOT NULL,
    price DECIMAL(38, 12) NOT NULL,
    commission DECIMAL(38, 12),
    currency VARCHAR,
    executed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE positions_current (
    account_id VARCHAR NOT NULL,
    instrument_id UUID NOT NULL,
    quantity DECIMAL(38, 12) NOT NULL,
    average_cost DECIMAL(38, 12),
    market_price DECIMAL(38, 12),
    market_value DECIMAL(38, 12),
    unrealized_pnl DECIMAL(38, 12),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, instrument_id)
);

CREATE TABLE dataset_files (
    file_id UUID PRIMARY KEY,
    dataset VARCHAR NOT NULL,
    relative_path VARCHAR NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    instrument_id UUID,
    timeframe VARCHAR,
    min_time TIMESTAMPTZ,
    max_time TIMESTAMPTZ,
    row_count BIGINT NOT NULL,
    byte_size BIGINT NOT NULL,
    checksum VARCHAR,
    active BOOLEAN NOT NULL,
    replaces_file_id UUID,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE backfill_jobs (
    job_id UUID PRIMARY KEY,
    request_json JSON NOT NULL,
    status VARCHAR NOT NULL,
    progress_json JSON NOT NULL,
    last_error JSON,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

还应包含 `schema_migrations`、`strategy_definitions`、`strategy_runs`、`signals`、`risk_decisions`、`account_snapshots`、`cash_balances_current` 和 `system_events`。

迁移 SQL 文件进入版本控制。启动时：

- 数据库版本高于程序支持版本则拒绝启动；
- 迁移前创建可配置备份；
- 每次迁移记录版本、checksum 和执行时间；
- 不允许已执行迁移文件的 checksum 变化。

## 7.8 分析与研究

DuckDB 通过 manifest 生成的视图查询 Parquet，例如：

```sql
SELECT instrument_id,
       time_bucket(INTERVAL '1 day', open_time) AS day,
       first(open ORDER BY open_time) AS open,
       max(high) AS high,
       min(low) AS low,
       last(close ORDER BY open_time) AS close,
       sum(volume) AS volume
FROM active_bars
WHERE instrument_id = $instrument_id
  AND open_time >= $start_time
  AND open_time < $end_time
GROUP BY instrument_id, day
ORDER BY day;
```

首版分析接口提供：

- 参数化只读 SQL 模板；
- OHLCV、收益率、波动率和移动窗口基础查询；
- 数据覆盖率和缺口报告；
- 策略回测输入读取；
- 结果保存为 DuckDB 表或导出 Parquet。

不允许通过远程 RPC 默认执行任意 SQL。个人本机模式可以提供 `analysis sql`，但必须：

- 仅允许只读连接；
- 拒绝 `ATTACH`、`INSTALL`、`LOAD`、文件写出等危险语句；
- 限制执行时间、扫描数据量和结果行数；
- 或更安全地只开放预定义 query API。

当 DuckDB SQL 对复杂滚动状态、机器学习前处理或 DataFrame 操作明显不便时，再在 `analysis` 模块引入 Polars。领域层和策略实时路径不依赖 Polars。

## 7.9 策略运行时

策略接口保持数据和交易通道分离：

```rust
#[async_trait::async_trait]
pub trait Strategy: Send {
    fn metadata(&self) -> StrategyMetadata;
    async fn on_start(&mut self, ctx: &StrategyContext) -> Result<()>;
    async fn on_event(
        &mut self,
        ctx: &StrategyContext,
        event: StrategyEvent,
    ) -> Result<Vec<OrderIntent>>;
    async fn on_stop(&mut self, ctx: &StrategyContext) -> Result<()>;
}
```

`StrategyContext` 只提供受控能力：

- 查询历史数据和当前组合；
- 发布指标和诊断信息；
- 提交订单意图；
- 获取时钟和确定性随机数源；
- 访问策略命名空间状态。

策略不能直接访问 `ibapi`、DuckDB 写连接或真实系统时钟。这样同一策略可以运行于：

- `Backtest`：历史事件驱动，使用模拟撮合；
- `Paper`：连接 IBKR paper account；
- `Live`：连接实盘账户。

每次启动生成 `StrategyRunId`，保存策略名称、代码版本、参数、数据版本、运行模式和开始/结束时间。策略任务 panic 或超时应被 host 捕获，停止该策略并发出告警；不能拖垮整个 daemon。

首版建议策略静态编译进程序。动态插件、WASM 或脚本语言会增加 ABI、安全和部署复杂度，可在接口稳定后再设计。

## 7.10 回测与模拟撮合

回测引擎按 `(event_time, sequence)` 确定性排序数据，避免未来函数：

- 策略在当前 bar final 后生成的订单，默认最早在下一 bar 撮合；
- 明确配置成交模型、滑点、手续费和交易时段；
- 限价/止损在 OHLC 内触发时采用保守且文档化的成交假设；
- 公司行动和复权口径与输入数据绑定；
- 随机行为使用记录 seed 的伪随机数生成器。

回测结果保存：

- 参数和代码版本；
- 数据文件 ID 或 dataset snapshot；
- 订单、成交和每日权益；
- 手续费、滑点、换手率、最大回撤等指标；
- 警告，例如缺失数据或无法确定的 bar 内成交顺序。

## 7.11 订单管理器

订单生命周期是资金安全的核心。内部状态机建议为：

```text
Created
  -> RiskRejected
  -> Approved
  -> Submitting
  -> Submitted
  -> PartiallyFilled
  -> Filled
  -> CancelPending
  -> Cancelled
  -> Rejected
  -> Unknown
```

`Unknown` 用于提交超时或断线时无法确认结果的情况。绝不能因为没有收到确认就自动重新提交；必须先按内部 ID、IBKR order ID、perm ID、开放订单和成交进行对账。

提交顺序：

1. RPC 或策略创建 `OrderIntent`，要求 `idempotency_key`；
2. 持久化 intent；
3. 读取账户/持仓/市场数据快照进行风险检查；
4. 持久化每条规则的决策和输入快照；
5. 分配内部 `OrderId`；
6. 使用 IBKR 提供的合法 order ID 发送；
7. 保存 broker ID 映射和所有状态事件；
8. 成交到达后去重，更新订单及持仓视图；
9. 定期与 IBKR 权威状态对账。

RPC 超时只表示客户端没有及时收到响应，不等于订单失败。客户端必须用 idempotency key 查询最终结果，不能换 key 重试。

组合订单和 parent/child order：

- 先完整校验整个订单图；
- 默认在所有子单构造完成后利用 transmit 语义一次激活；
- 持久化 parent/child 和 OCA 关系；
- 部分提交失败时进入人工确认状态。

## 7.12 风险引擎

风险检查是独立组件，并且不可由策略绕过。首版规则：

- 全局 `trading_enabled` 和紧急停止开关；
- 只允许配置白名单账户、合约、证券类型和交易所；
- 单笔最大数量和名义价值；
- 单合约最大净持仓；
- 组合最大总敞口；
- 当日最大已实现/未实现亏损；
- 最大开放订单数和单位时间订单数；
- 价格偏离最新可信行情的最大比例；
- 行情陈旧检查；
- 只允许配置的交易时段，是否允许盘前盘后；
- 交易时段来自 IBKR `ContractDetails`：正常时段使用 `liquidHours`，扩展时段使用
  `tradingHours`，按 `timeZoneId` 转换 UTC 并缓存；缺失或过期且刷新失败时拒绝下单；
- 重复订单/idempotency 检查；
- IBKR 未就绪、账户数据陈旧或对账未完成时禁止开仓；
- 可配置只允许平仓模式。

风险结果分为 `Allow`、`Reject`、`RequireManualApproval`。每次结果保存规则版本、输入快照、原因代码和文本说明。

紧急停止分级：

- `pause_strategies`：停止新策略事件；
- `reject_new_orders`：拒绝所有新订单，仍允许撤单；
- `cancel_open_orders`：需要显式二次确认；
- `flatten_positions`：高风险破坏性操作，必须显式指定账户、合约范围和订单类型，默认不提供自动触发。

## 7.13 账户、持仓与对账

IBKR 是账户、开放订单和成交的最终权威来源，本地库是可审计的镜像和内部意图来源。

对账触发：

- daemon 启动和每次重连；
- 固定周期；
- RPC 手动触发；
- 发现未知订单、状态倒退或持仓不一致时。

对账结果分类：

- 本地与 IBKR 一致；
- IBKR 存在本地未知订单；
- 本地活动订单在 IBKR 中缺失；
- 成交缺失或重复；
- 持仓数量/成本不一致；
- 账户或币种映射不一致。

不一致时默认进入 `Degraded` 并禁止自动开仓。未知外部订单不会自动撤销或接管；记录并要求配置或人工处理。

## 7.14 JSON-RPC 服务

协议采用 JSON-RPC 2.0，由 `jsonrpsee` 提供服务端和客户端。daemon 仅监听
`127.0.0.1:8787`：CLI 使用 HTTP client，Yew WASM 使用 WebSocket client。项目不再
支持 Unix Domain Socket，也不定义自有裸 TCP framing。

可选 TCP 监听默认关闭。启用 TCP 时必须配置：

- 只监听 loopback，或启用 TLS；
- bearer token 或更强认证；
- 请求大小、并发和速率限制；
- 禁止默认暴露任意 SQL 和敏感配置。

方法按命名空间组织：

| 方法 | 类型 | 作用 |
|---|---|---|
| `system.status` | query | 健康、版本、IBKR 和组件状态 |
| `system.shutdown` | command | 优雅停止 |
| `ibkr.connect` / `ibkr.disconnect` | command | 管理连接 |
| `instrument.search` | query | 查询 IBKR 合约候选 |
| `instrument.add` / `instrument.list` | command/query | 管理确认合约 |
| `market.subscribe` / `market.unsubscribe` | command | 管理实时行情 |
| `data.backfill.start` | command | 创建历史补齐任务 |
| `data.job.get` / `data.job.list` | query | 查询任务进度 |
| `data.gaps` | query | 查询数据缺口 |
| `strategy.start` / `strategy.stop` | command | 管理策略 |
| `strategy.list` / `strategy.run.get` | query | 查询策略状态 |
| `order.preview` | query | 仅运行风险检查 |
| `order.submit` | command | 提交订单意图 |
| `order.cancel` | command | 撤单 |
| `order.get` / `order.list` | query | 查询订单 |
| `account.summary` | query | 账户汇总 |
| `portfolio.positions` | query | 当前持仓 |
| `risk.status` / `risk.set_mode` | query/command | 风险状态与模式 |
| `reconcile.run` | command | 启动对账 |
| `analysis.run` | command | 启动受控分析任务 |

请求示例：

```json
{
  "jsonrpc": "2.0",
  "id": "01J3...",
  "method": "order.submit",
  "params": {
    "idempotency_key": "manual-20260727-001",
    "account_id": "DU123456",
    "instrument_id": "0190...",
    "side": "buy",
    "quantity": "10",
    "order_type": "limit",
    "limit_price": "185.20",
    "time_in_force": "day",
    "outside_rth": false,
    "dry_run": false
  }
}
```

成功响应返回“已接受的内部对象”，不虚假承诺券商已接受：

```json
{
  "jsonrpc": "2.0",
  "id": "01J3...",
  "result": {
    "order_intent_id": "0190...",
    "order_id": "0190...",
    "status": "submitting",
    "accepted_at": "2026-07-27T05:10:00.123456Z"
  }
}
```

错误对象：

```json
{
  "jsonrpc": "2.0",
  "id": "01J3...",
  "error": {
    "code": -32020,
    "message": "risk check rejected",
    "data": {
      "reason_code": "MAX_POSITION_EXCEEDED",
      "correlation_id": "0190...",
      "retryable": false
    }
  }
}
```

错误码按区段划分：参数错误、认证/授权、状态冲突、IBKR 不可用、风险拒绝、存储错误、超时和内部错误。响应不能泄露 token、完整账户敏感信息或 Rust backtrace。

所有有副作用的 RPC 都要求或生成 idempotency key。RPC 层记录 method、耗时、结果码和 correlation ID，但不完整记录可能含敏感数据的请求体。

实时事件首版可以通过 `event.poll(cursor, timeout)` 长轮询；后续若需要交互式 UI，可增加 WebSocket subscription，不改变命令方法。

## 7.15 CLI 设计

CLI 只做参数解析、请求发送和输出格式化，业务逻辑保留在 daemon。

```text
quant status [--json]
quant instrument search AAPL --exchange SMART --currency USD
quant instrument add --conid 265598
quant data backfill --instrument <uuid> --bar-size 1m \
  --from 2026-01-01T00:00:00Z --to 2026-07-01T00:00:00Z
quant data jobs
quant strategy start mean-reversion --param lookback=20 --mode paper
quant strategy stop <run-id>
quant order preview --instrument <uuid> --side buy --quantity 10 \
  --limit 185.20
quant order submit ... --idempotency-key <key> --confirm
quant order cancel <order-id> --confirm
quant account summary
quant positions
quant reconcile
quant shutdown
```

输出模式：

- TTY 默认人类可读表格；
- `--json` 输出稳定 JSON，方便脚本使用；
- 错误写 stderr，正常结果写 stdout；
- 退出码区分参数错误、daemon 不可达、业务拒绝和内部错误；
- 金额和数量作为字符串输出，避免 shell/JSON 消费者丢失精度。

危险操作使用明确动词并要求 `--confirm`。非交互脚本必须同时提供 idempotency key；不能通过模糊的 yes/no stdin 造成不可重放行为。

## 7.16 配置

配置加载优先级：

1. 内置安全默认值；
2. TOML 文件；
3. `QUANT__SECTION__KEY` 格式环境变量；
4. 明确的 CLI 启动参数。

示例：

```toml
[app]
environment = "paper"
data_dir = "./data"
timezone = "UTC"

[ibkr]
host = "127.0.0.1"
port = 4002
client_id = 17
account = "DU123456"
connect_on_start = true
request_timeout_seconds = 30
reconnect_max_seconds = 60
readonly = false

[rpc]
http_listen = "127.0.0.1:8787"
tcp_enabled = false
max_request_bytes = 1048576
max_concurrent_requests = 32
request_timeout_seconds = 30

[storage]
duckdb_path = "./data/state.duckdb"
lake_dir = "./data/lake"
staging_dir = "./data/staging"
parquet_compression = "zstd"
parquet_target_file_mib = 128
write_batch_rows = 10000
flush_interval_seconds = 5

[logging]
level = "info"
format = "json"
directory = "./data/logs"
rotation = "daily"
retain_days = 30

[risk]
trading_enabled = false
mode = "reject_new_orders"
allowed_security_types = ["stock", "etf"]
allowed_currencies = ["USD"]
max_order_notional = "10000"
max_instrument_position_notional = "25000"
max_gross_exposure = "100000"
max_daily_loss = "2000"
max_market_data_age_seconds = 5
max_orders_per_minute = 10
allow_outside_rth = false

[strategy]
auto_start = []
event_queue_capacity = 4096
handler_timeout_seconds = 2
```

启动校验：

- `app.timezone` 必须为 UTC；其他时区只可用于展示；
- paper/live 端口和账户环境不能明显冲突；
- 实盘环境默认 `trading_enabled = false`，需显式开启；
- 路径规范化后必须位于允许的数据目录；
- 风险阈值必须为正且逻辑一致；
- 配置日志输出要脱敏。

认证 token 等秘密不建议明文写 TOML；后续 TCP 模式通过环境变量或权限受限的单独 secret 文件加载。

首版不做全量热加载。日志级别等无状态配置可以重新加载；账户、数据库路径、IBKR client ID、风险上限等关键配置变更要求重启，或通过专用 RPC 原子更新并审计。

## 7.17 日志、指标与健康检查

使用 `tracing`：

- `tracing-subscriber` 配置过滤和 JSON/pretty 格式；
- `tracing-appender` 做非阻塞轮转文件；
- 所有 RPC、IBKR 请求、数据任务、策略 run 和订单流使用 span；
- 统一字段：`correlation_id`、`request_id`、`order_id`、`strategy_run_id`、`instrument_id`、`conid`；
- 禁止日志记录认证信息；账户号默认掩码；
- 订单和风险审计落数据库，日志不是唯一审计源。

关键指标：

- IBKR 连接状态、重连次数、API 错误数；
- 各 channel 深度、丢弃/合并行情数；
- RPC 请求数、错误率和延迟；
- 历史请求数、pacing 等待和任务进度；
- DuckDB 写队列深度、事务耗时；
- Parquet 行数、文件数、临时文件数；
- 行情最新时间与数据延迟；
- 订单各状态数量、拒绝和未知订单数；
- 对账差异数；
- 策略事件耗时、超时和错误数。

`system.status` 返回：

- `process`: pid、版本、启动时间、uptime；
- `state`: Starting/Synchronizing/Ready/Degraded/Draining；
- `ibkr`: 连接、账户、最后事件时间；
- `storage`: DuckDB、Parquet 和写队列健康；
- `market_data`: 活跃订阅和最大延迟；
- `trading`: 是否允许下单、风险模式；
- `reconciliation`: 最近完成时间和差异数。

## 7.18 错误处理

定义分层错误：

- `ConfigError`;
- `ValidationError`;
- `IbkrError`;
- `StorageError`;
- `RiskError`;
- `RpcError`;
- `StrategyError`;
- `ReconciliationError`.

使用 `thiserror` 定义库级错误；应用入口可用 `anyhow` 聚合上下文。每个错误标注：

- 稳定的机器可读 code；
- 是否可重试；
- 是否影响交易就绪状态；
- 用户可见的安全消息；
- 内部 source chain。

重试只用于幂等操作，或能够证明未产生外部副作用的操作。下单超时不能简单重试。永久错误进入失败状态并等待配置或人工处理。

## 8. 一致性、幂等与崩溃恢复

系统无法在本地 DuckDB 与 IBKR 之间建立分布式事务，因此采用“持久化意图 + 幂等键 + 对账”：

1. 本地事务持久化订单意图；
2. 发送到 IBKR；
3. 持久化确认和状态事件；
4. 若第 2/3 步之间崩溃，重启后先查询 IBKR 状态；
5. 根据 broker order ID、perm ID、order ref 和成交记录对账；
6. 无法判断时标为 `Unknown`，禁止自动重发。

建议将内部 `OrderId` 的短格式写入 IBKR `order_ref`（在字段限制允许时），增强对账能力。

Parquet 原子提交协议：

1. 在 staging 目录写唯一临时文件；
2. flush、close 并读取校验 schema/行数/时间范围；
3. 可选 fsync；
4. rename 到最终目录；
5. DuckDB 事务插入新 manifest，并将被替代文件置 inactive；
6. 崩溃恢复扫描临时文件和无 manifest 的最终文件，移入 quarantine 或重新登记，不直接加入查询。

DuckDB 定期备份。备份不等于 Parquet 备份：数据湖、数据库、配置和策略参数需要作为一个恢复集管理，manifest 中只保存相对路径以便迁移。

## 9. 并发与背压

推荐任务边界：

- IBKR 专用 actor/thread；
- Storage writer 专用阻塞任务/thread；
- RPC server；
- 每个策略一个受监督 task；
- 行情聚合 task；
- 历史数据 scheduler；
- reconciliation task；
- compaction/retention task。

所有 channel 有界并有明确拥塞策略：

| 数据 | 拥塞策略 |
|---|---|
| 订单、成交、风险事件 | 不丢弃；触发降级和停止接收新订单 |
| 账户和持仓 | 不丢弃最终快照；可合并中间更新 |
| 实时 tick | 按订阅声明：阻塞、合并最新值或计数丢弃 |
| final bar | 默认不丢弃；拥塞时暂停上游策略输入 |
| 日志 | 非阻塞写；记录丢失指标 |
| RPC 通知 | 慢客户端断开或要求从 cursor 重读 |

CPU 密集分析、DuckDB 查询、Parquet 编解码均使用 `spawn_blocking` 或独立计算池，避免阻塞 Tokio reactor。必须限制并发分析数，防止研究查询影响实盘路径。

## 10. 安全设计

- daemon RPC 安全默认监听 loopback TCP；显式配置外部接口时必须增加防火墙、TLS、
  身份认证和访问控制；
- 数据目录、配置和日志权限仅限当前用户；
- 实盘必须显式配置账户白名单和开启交易；
- CLI 默认先 `order preview`，实际提交需要明确确认；
- 所有副作用 RPC 审计 method、主体、idempotency key 和结果；
- 任意 SQL、DuckDB extension 安装、任意文件导入默认关闭；
- 配置和日志脱敏账户、token 和连接信息；
- 不在 RPC 返回 panic/backtrace；
- TWS/IB Gateway 本身应只监听必要接口，启用 IBKR 的可信 IP 和只读设置时要与平台模式一致；
- 定期校验二进制版本、配置 checksum 和数据库迁移版本；
- `flatten_positions`、批量撤单和实盘模式切换设计为显式、高摩擦操作。

## 11. 测试方案

### 单元测试

- 时间和 IBKR 时区解析，覆盖夏令时切换；
- 合约映射和 Decimal 精度；
- 订单状态机所有合法/非法转换；
- 风险规则边界；
- K 线聚合与数据质量校验；
- pacing scheduler；
- 配置默认值、覆盖和校验；
- RPC 类型序列化兼容性。

### 属性测试

使用 `proptest` 验证：

- 任意订单事件序列不会使 filled quantity 倒退或超过允许范围；
- bar 聚合满足 OHLC 不变量；
- 相同 idempotency key 不会创建两个订单意图；
- 历史分片计划无缺口且边界规则一致。

### 集成测试

- 临时 DuckDB 的迁移和事务；
- struct → Parquet → DuckDB 查询往返；
- manifest 替换和 compaction；
- JSON-RPC server/client 端到端；
- daemon 重启后的任务恢复；
- 模拟 IBKR adapter 的断线、乱序、重复事件和提交超时；
- 订单对账的未知订单、丢失成交和状态冲突。

### IBKR 环境测试

- 使用 TWS/IB Gateway paper account；
- 合约解析、历史数据、实时行情；
- 提交、改单、撤单、部分成交；
- TWS 重启和网络中断；
- client ID 冲突；
- pacing violation；
- 跨交易日和夏令时；
- 手工从 TWS 创建订单后进行外部订单对账。

测试不能依赖实盘账户。实盘启用前执行小额度、白名单合约的人工验收清单。

### 故障注入

- Parquet 写一半进程退出；
- 最终文件 rename 后、manifest 提交前退出；
- 订单发送后、确认保存前退出；
- DuckDB 写满/磁盘空间不足；
- channel 满；
- 系统时间跳变；
- IBKR 重复或乱序状态；
- RPC 客户端超时后重试。

## 12. 性能与容量规划

个人平台初期目标：

- 数十至数百个合约的分钟数据；
- 少量实时订阅和策略；
- 单机数年历史数据；
- 非高频订单。

优化顺序：

1. 用指标确认瓶颈；
2. 批量 DuckDB 写入和 Parquet 写入；
3. 合理 Parquet 分区、predicate pushdown 和小文件合并；
4. 限制 RPC 分析查询；
5. 将 CPU 密集任务移出 Tokio worker；
6. DuckDB SQL 无法自然表达时引入 Polars；
7. 只有在跨模块列式传输成为实际瓶颈时才将 Arrow 引入内存公共接口。

不应过早用复杂无锁结构替代有界 channel 和清晰 actor 所有权。

## 13. 版本与兼容性

需要分别管理：

- 配置 schema version；
- DuckDB migration version；
- Parquet schema version；
- JSON-RPC API version；
- 策略参数 schema version；
- 应用 semantic version 和 Git commit。

RPC 可通过 `system.version` 返回能力列表。新增字段保持向后兼容；删除或重命名方法通过新的 API major version。Parquet 读取器需要支持当前和至少一个旧 schema，升级由离线 migration/compaction 完成。

## 14. 分阶段实施计划

### 阶段 1：基础骨架

- CLI/daemon 双模式；
- TOML 配置和校验；
- tracing、进程锁、优雅停止；
- DuckDB 连接、迁移和单写者；
- JSON-RPC TCP 的 `system.status`/`shutdown`；
- 领域 ID、时间和错误类型。

验收：daemon 可长期运行，第二实例被拒绝，CLI 可查询状态并优雅关闭。

### 阶段 2：IBKR 与主数据

- IBKR actor；
- 连接状态机、重连和 pacing；
- 合约搜索与确认；
- 账户、持仓和基础对账；
- 结构化错误与指标。

验收：paper account 断线重连后恢复 Ready，合约和账户状态可查询。

### 阶段 3：历史与实时数据

- 历史 backfill scheduler；
- Parquet writer、manifest 和崩溃恢复；
- DuckDB 查询视图和 gap 检测；
- 实时订阅与 bar 聚合；
- 数据质量测试。

验收：重复执行 backfill 不产生逻辑重复，重启可续传，SQL 可查询跨文件数据。

### 阶段 4：订单与风险

- OrderIntent 和状态机；
- 风险引擎；
- paper 下单、撤单、成交和手续费；
- idempotency 与订单对账；
- 紧急停止模式。

验收：提交超时/断线不会重复下单，所有风险决策和状态变更可审计。

### 阶段 5：策略与回测

- 策略 trait 和 host；
- 内置示例策略；
- 确定性回测和模拟撮合；
- strategy run 元数据、指标和结果；
- paper 自动运行。

验收：同一数据和 seed 的回测结果可重复，策略不能绕过风险路径。

### 阶段 6：生产化

- 备份恢复演练；
- compaction 和 retention；
- 性能压测和故障注入；
- RPC 兼容性和安全审查；
- systemd/launchd 服务文件；
- 实盘人工验收与最小额度试运行。

## 15. 建议依赖

实现阶段可评估以下 crate：

```toml
[dependencies]
tokio = { version = "...", features = ["rt-multi-thread", "macros", "signal", "sync", "time"] }
tokio-util = "..."
ibapi = "..."
duckdb = { version = "...", features = ["bundled"] }
parquet = "..."
chrono = { version = "...", features = ["serde"] }
chrono-tz = "..."
uuid = { version = "...", features = ["v7", "serde"] }
serde = { version = "...", features = ["derive"] }
serde_json = "..."
toml = "..."
clap = { version = "...", features = ["derive"] }
jsonrpsee = { version = "...", features = ["server", "client"] }
tracing = "..."
tracing-subscriber = { version = "...", features = ["env-filter", "json"] }
tracing-appender = "..."
thiserror = "..."
anyhow = "..."
rust_decimal = { version = "...", features = ["serde"] }
async-trait = "..."
```

具体 feature 和版本应在写第一个可编译垂直切片时确定。若 `duckdb` 与 `parquet` 依赖的 Arrow 版本发生冲突，应保持 Arrow 类型只存在于存储实现内部，必要时选择不暴露 Arrow 的写入方式或统一依赖版本。

## 16. 关键设计决策摘要

1. **单 daemon 所有权**：只有后台进程连接 IBKR 和写数据库，CLI 只走 RPC。
2. **阻塞边界隔离**：`ibapi`、DuckDB 和重型分析不阻塞 Tokio worker。
3. **交易单一路径**：任何订单都必须经过持久化意图、风险引擎和订单管理器。
4. **对账优先于猜测**：外部调用结果不确定时标记 Unknown，先对账，不盲目重试。
5. **DuckDB + Parquet 分工**：DuckDB 管元数据、当前状态和分析，Parquet 管大规模历史时序数据。
6. **UTC 和类型安全 ID**：所有时刻统一 UTC，内部 ID 与 IBKR conid/order ID 明确分离。
7. **普通 Rust struct 为核心**：Arrow/Polars 仅在实际需要的边界引入。
8. **安全默认值**：loopback RPC、实盘默认禁用交易、危险操作显式确认。
9. **可恢复写入**：Parquet 原子提交、DuckDB 迁移、任务 checkpoint 和启动对账。
10. **先做纵向闭环**：优先完成“连接 → 数据 → 风险 → paper 下单 → 成交 → 持久化 → RPC 查询”的小闭环，再扩展资产类别和分析能力。

## 17. 首个可交付垂直切片

建议首个真正可运行版本只实现：

- `quant daemon` 与 `quant status/shutdown`；
- paper IB Gateway 连接和重连；
- 搜索并保存一个股票合约；
- 下载该合约一个月的 1 分钟 bar；
- 原子写入 Parquet，并通过 DuckDB SQL 查询；
- 查询账户与持仓；
- preview 风险检查；
- 提交一笔极小的 paper 限价单、撤单并完整记录状态；
- daemon 重启后对账并恢复正确状态。

这个切片会验证架构中风险最高的边界：Tokio 与 `ibapi`、DuckDB 并发、Parquet 提交、RPC 幂等、订单状态机和重连对账。完成后再增加策略与回测，能够显著降低后续返工。
