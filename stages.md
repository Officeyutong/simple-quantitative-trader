# 量化交易平台开发阶段

## 1. 文档说明

本文汇总基于 IBKR API 的个人量化交易平台从设计到完整交付的全部开发阶段，并记录当前实际进度。

状态定义：

- **已完成**：核心代码已经实现，通过编译和测试，并完成相应的 paper Gateway 验收；
- **部分完成**：主要路径可用，但仍存在明确的可靠性、完整性或运维缺口；
- **未开始**：仅在 `design.md` 中完成设计，尚未进入实现；
- **后期扩展**：不属于首个稳定版本的必需功能。

当前平台定位：

> 已经能够连接 IBKR、采集历史行情、保存 Parquet、查询账户持仓、通过风险检查提交和撤销股票/ETF paper 订单，并保存订单、成交和手续费。当前仍处于 paper 验证阶段，不适合无人值守实盘。

完整架构设计参见 [design.md](design.md)，使用方法参见 [README.md](README.md)。

## 2. 阶段总览

| 阶段 | 名称 | 当前状态 |
|---|---|---|
| 0 | 需求与架构设计 | 已完成 |
| 1 | daemon、CLI、配置和存储基础 | 已完成 |
| 2 | IBKR 连接与账户发现 | 已完成 |
| 3 | 合约搜索与证券主数据 | 已完成（首版） |
| 4 | 账户、持仓和组合快照 | 已完成（首版） |
| 5 | 历史行情与 Parquet 数据湖 | 已完成（首版） |
| 6 | 订单提交、撤单与基础风险控制 | 已完成 |
| 7 | 订单、成交和手续费持续同步 | 已完成 |
| 8 | 启动、重连与状态对账 | 已完成（首版） |
| 9 | 实时行情服务 | 已完成（首版） |
| 10 | 完整风险引擎与交易就绪门控 | 已完成（首版） |
| 11 | 策略运行器 | 已完成（信号模式） |
| 12 | 回测与模拟撮合 | 已完成（首版） |
| 13 | 数据治理与批量分析 | 已完成（首版） |
| 14 | 监控、备份和生产化部署 | 已完成（首版） |
| 15 | 实盘准入与渐进发布 | 门控已完成，实盘未批准 |
| 16 | 多资产与高级订单扩展 | 不属于首个稳定版 |

## 3. 阶段 0：需求与架构设计

**状态：已完成**

### 已完成内容

- 明确使用 Rust 和 Tokio；
- 使用 `ibapi` 连接 TWS/IB Gateway；
- 使用 DuckDB 作为嵌入式分析数据库；
- 使用 Parquet 保存历史文件；
- 核心内存数据使用普通 Rust struct；
- 时间统一使用 `chrono::DateTime<Utc>`；
- 内部实体使用 UUID，券商合约保留 IBKR conid；
- 日志使用 `tracing`；
- 配置使用 TOML 和 `serde`；
- daemon 通过 JSON-RPC 对外提供服务；
- 同一二进制提供 CLI；
- 完成订单状态机、风险、数据湖、对账、策略和回测的总体设计。

### 交付物

- [design.md](design.md)

## 4. 阶段 1：daemon、CLI、配置和存储基础

**状态：已完成**

### 已完成内容

- 同一二进制支持 daemon 和 CLI 两种运行模式；
- TOML 配置加载；
- 环境变量覆盖；
- 严格配置字段校验；
- UTC 强制校验；
- `tracing` pretty/JSON 日志；
- daemon 进程互斥锁；
- loopback TCP JSON-RPC（当前已由后续阶段替代 UDS）；
- Socket 权限设置为 `0600`；
- 残留 Socket 清理；
- JSON-RPC 2.0 请求和响应；
- RPC 请求大小、超时及并发限制；
- `system.status`；
- `system.version`；
- `system.shutdown`；
- Ctrl-C 和 RPC 优雅停止；
- DuckDB 自动创建和 migration；
- 数据湖和 staging 目录初始化。

### 验收结果

- 第二份 daemon 无法占用相同数据目录；
- CLI 可以查询 daemon 状态；
- CLI 可以优雅关闭 daemon；
- DuckDB schema migration 测试通过；
- daemon/CLI JSON-RPC 端到端测试通过。

## 5. 阶段 2：IBKR 连接与账户发现

**状态：已完成**

### 已完成内容

- 引入 `ibapi 3.3`；
- 使用其 Tokio 异步客户端；
- 单独的 IBKR actor 独占 `ibapi::Client`；
- 有界命令队列；
- 连接状态机：
  - `disconnected`
  - `connecting`
  - `ready`
  - `reconnecting`
  - `stopping`
- 手动连接和断开；
- `connect_on_start`；
- 连接超时；
- 指数退避重连；
- 自动检测连接丢失；
- 获取 IBKR server version；
- 获取 managed accounts；
- 可选账户白名单检查；
- daemon 停止时主动断开 IBKR。

### RPC/CLI

```text
ibkr.status
ibkr.connect
ibkr.disconnect
account.managed
```

```bash
quant ibkr status
quant ibkr connect
quant ibkr disconnect
quant account managed
```

### Paper 验收结果

- 成功连接本机 IB Gateway；
- server version 为 225；
- managed account 获取成功；
- Read-Only API 拒绝可以被正确识别；
- Gateway 关闭 Read-Only 后可以提交 paper 订单。

## 6. 阶段 3：合约搜索与证券主数据

**状态：已完成（首版）**

### 已完成内容

- 使用 IBKR `matching_symbols` 搜索合约；
- 返回：
  - conid
  - symbol
  - security type
  - currency
  - exchange
  - primary exchange
  - local symbol
  - description
  - derivative security types
- 当前持仓中的合约自动写入 `instruments` 表；
- 下单和历史行情请求使用 conid。

### RPC/CLI

```text
instrument.search
```

```bash
quant instrument search AAPL
```

### 剩余工作

- 明确的 `instrument.add`/`instrument.remove`；
- 使用内部 `InstrumentId` 下单，而不是每次重复传递合约字段；
- 保存完整 contract details；
- 保存 min tick、multiplier、trading class 和交易所时区；
- 保存有效路由和 Overnight 可交易性；
- 合约快照版本管理；
- 防止 symbol 相同但合约不同造成误选。

首版增量已经提供持久化内部 `instrument_id`、唯一 conid、完整搜索候选合约字段和
`instrument.list`。搜索结果自动 upsert，不再只存在于单次 RPC 响应中。以上剩余项
作为多资产与高级路由扩展，不阻塞股票/ETF 首个稳定版。

### 完成标准

- 所有行情和订单 API 只接受内部 `InstrumentId`；
- conid 和完整 IBKR contract 快照由证券主数据服务统一管理；
- 模糊合约不允许自动交易。

## 7. 阶段 4：账户、持仓和组合快照

**状态：已完成（首版）**

### 已完成内容

- managed accounts 查询；
- 当前持仓持续订阅；
- 持仓数据包括：
  - account
  - conid
  - symbol
  - security type
  - currency
  - exchange
  - quantity
  - average cost
  - observed time
- 持仓变化实时写入 DuckDB `positions_current`；
- IBKR Ready 后自动订阅全部账户摘要标签；
- 账户摘要写入 `account_summary_current`；
- 覆盖 NetLiquidation、AvailableFunds、BuyingPower、保证金和多币种 ledger；
- 为每个 managed account 建立账户级 PnL 订阅；
- daily、unrealized 和 realized PnL 写入 `account_pnl_current`；
- 主动断开或连接丢失时取消旧订阅；
- 重连后自动重建账户、持仓和 PnL 订阅；
- paper 买卖测试通过持仓变化确认成交。

### RPC/CLI

```text
portfolio.positions
account.summary
account.pnl
```

```bash
quant positions
quant account summary
quant account pnl
```

### 剩余工作

- 账户和持仓历史快照；
- 每日权益曲线；
- 多账户隔离；
- 外部手工持仓变化检测。

schema 15 新增 `position_history` 与 `account_pnl_history`，持续事件同时更新 current
视图和不可变历史。多账户数据始终以 account ID 隔离；外部变化由 IBKR 持仓流和重连
对账捕获。

schema 18 增加持仓快照 start/end 状态：每轮 IBKR positions 同步开始时进入
`syncing` 并阻止开仓，结束时将本轮未出现的旧持仓归零、刷新 observed time 后进入
`ready`。2026-07-28 Gateway 回归测试确认零仓位 SPY 的时间随新 session 刷新。

### Paper 验收结果

- DuckDB 自动升级到 schema version 5；
- 获取完整账户摘要和 HKD、USD、BASE 多币种 ledger；
- 获取账户 daily、unrealized、realized PnL；
- 获取 SPY 零持仓的持续快照；
- 主动断开重连后 connection session ID 轮换；
- PnL `observed_at` 在重连后刷新，证明订阅已自动重建；
- 重连对账保持 `healthy`。

## 8. 阶段 5：历史行情与 Parquet 数据湖

**状态：已完成（首版）**

### 已完成内容

- IBKR 历史行情请求；
- 支持：
  - 1 分钟
  - 5 分钟
  - 15 分钟
  - 30 分钟
  - 1 小时
  - 1 日
- 明确 UTC 起止时间；
- 支持 regular hours 和 outside RTH；
- 基础行情质量检查：
  - OHLC 不变量
  - 有限数值
  - 非负成交量
  - 时间严格递增
  - 单批次合约和周期一致
- 使用 DuckDB `COPY ... FORMAT PARQUET` 写文件；
- ZSTD 压缩；
- staging 临时文件；
- 完成写入后原子 rename；
- 按 timeframe 和 conid 分区；
- `dataset_files` manifest；
- 文件行数、大小、最小/最大时间记录。

### RPC/CLI

```text
data.backfill
```

```bash
quant data backfill ...
```

### Paper 验收结果

AAPL 日线下载成功：

```text
data/lake/bars/timeframe=1d/conid=265598/
```

实测文件包含 5 行、ZSTD Parquet 数据。

### 剩余工作

- 大时间范围自动切片；
- IBKR pacing token bucket；
- 后台 job 和进度查询；
- 失败重试和重启续传；
- 数据覆盖范围；
- 缺口检测和自动补齐；
- 重复业务键去重；
- 重复 backfill 文件失效；
- Parquet 小文件合并；
- manifest 崩溃恢复；
- quarantine；
- 交易日历；
- 公司行动和复权；
- Tick 数据集；
- DuckDB active dataset 统一视图。

### 后续增量（已完成）

- DuckDB schema version 10；
- 历史 backfill 持久化为 `data_jobs`；
- daemon Ready 后后台执行；
- 按 timeframe 自动切片；
- 单 worker 串行请求形成基础 pacing；
- 每个切片失败最多重试 3 次；
- cursor 和 completed slices 持久化；
- daemon 中断时 running Job 自动恢复为 retrying；
- `data jobs` 查询状态和错误；
- `data coverage` 查询文件覆盖和 raw gaps；
- Paper Job 成功写入 2 行 SPY 日线 Parquet；
- 2026-07-20 至 2026-07-22 coverage 验收无 raw gap。

## 9. 阶段 6：订单提交、撤单与基础风险控制

**状态：已完成**

### 已完成内容

- 股票/ETF `STK`；
- 买入和卖出；
- 市价单；
- 限价单；
- `outside_rth`；
- `OVERNIGHT` 直接路由；
- 风险预览；
- 实际提交要求 `--confirm`；
- 撤单要求 `--confirm`；
- idempotency key；
- 订单意图先持久化；
- 风险决策持久化；
- IBKR order ID 保存；
- broker rejection 保存；
- 订单提交失败不会再误报成功。

### 当前风险规则

- 全局 `trading_enabled`；
- 账户必须属于当前 IBKR 会话；
- 仅允许 `STK`；
- 数量必须为正且有限；
- 最大单笔数量；
- 最大单笔名义价值；
- 市价单风险检查需要 estimated price；
- 幂等键不能重复使用。

### RPC/CLI

```text
order.preview
order.submit
order.cancel
```

```bash
quant order preview ...
quant order submit ... --confirm
quant order cancel ... --confirm
```

### Paper 验收结果

- AAPL 常规/扩展时段下单和撤单测试通过；
- Read-Only API 错误 321 正确返回；
- Overnight 直接路由警告 10329 正确返回；
- IBKR 价格保护拒绝正确返回；
- SPY Overnight 买卖成交闭环通过。

## 10. 阶段 7：订单、成交和手续费持续同步

**状态：已完成**

### 已完成内容

- 下单首次确认后保留 IBKR subscription；
- 持续消费：
  - `OrderStatus`
  - `ExecutionData`
  - `CommissionReport`
- 独立 broker event channel；
- 独立 DuckDB 持久化消费者；
- 更新：
  - order status
  - filled quantity
  - average fill price
  - perm ID
- 保存：
  - execution ID
  - conid
  - side
  - quantity
  - price
  - commission
  - currency
- execution ID 去重；
- 订单和成交查询。

### RPC/CLI

```text
order.list
execution.list
```

```bash
quant order list
quant executions
```

### Paper 验收结果

SPY Overnight 成交：

| 方向 | IBKR order ID | 状态 | 数量 | 成交价 | 手续费 |
|---|---:|---|---:|---:|---:|
| Bought | 6 | Filled | 1 | 745.70 | 1.000003 USD |
| Sold | 7 | Filled | 1 | 745.59 | 1.015557 USD |

最终 SPY 净持仓为 0，订单、成交价和手续费均写入 DuckDB。

### 仍需增强

- 进程级全局 order update stream；
- 外部订单事件；
- 部分成交专项测试；
- commission 先于 execution 到达时的暂存处理；
- execution correction；
- 撤单最终状态持续确认；
- 组合订单事件。

## 11. 阶段 8：启动、重连与状态对账

**状态：已完成（首版）**

### 已完成内容

- 获取当前开放订单；
- 获取当日 execution 和 commission；
- IBKR 进入 `Ready` 后自动对账；
- 手动对账；
- 对账事件重新写入 DuckDB；
- 开放订单状态更新；
- unresolved 本地订单只报告、不自动重发；
- 对账不会再盲目把旧订单改为 `Unknown`。
- 每次 IBKR `Ready` 连接生成 UUIDv7 connection session ID；
- session ID 随连接状态、订单、成交持久化；
- 实时订单事件使用 `(connection_session_id, broker_order_id)` 关联；
- 重连对账使用 IBKR perm ID 跨会话关联；
- execution 使用 IBKR execution ID 全局去重；
- 撤单只允许命中当前连接会话中的订单。
- 使用 `all_open_orders` 发现其他 API client/TWS 创建的开放订单；
- 获取 IBKR completed orders 并使用 perm ID 关联本地订单；
- 外部开放及已完成订单保存到 `broker_order_snapshots`；
- 每次对账保存到 `reconciliation_runs`；
- 差异保存到 `reconciliation_differences`；
- 外部 completed order 记为 informational；
- 外部开放订单和本地活跃订单缺失记为 blocking；
- blocking difference 自动进入 `Degraded`；
- 新订单提交要求当前连接会话最近一次对账为 `healthy`；
- 对账为 `pending` 或 `degraded` 时禁止提交新订单；
- 撤单不受交易就绪门控影响。

### RPC/CLI

```text
reconcile.run
reconcile.status
reconcile.differences
reconcile.acknowledge
```

```bash
quant reconcile
quant reconcile status
quant reconcile differences
quant reconcile acknowledge --difference-id <UUID> --note <说明>
```

### Paper 验收结果

daemon 重启后：

- IBKR Ready；
- 自动恢复 8 个当日 broker events；
- 恢复之前 SPY 成交和手续费；
- 数据库已有 execution 按 broker execution ID 去重。
- DuckDB 自动升级到 schema version 3；
- 首次连接 session ID 为 `019fa27a-b167-7ec3-bc03-e322f8e09be0`；
- 主动断开重连后 session ID 轮换为 `019fa27b-6ce6-7df3-b444-2ff62a47df8a`；
- 重连后恢复 8 个 broker events，未解析本地订单为 0。
- DuckDB 自动升级到 schema version 4；
- 恢复 6 个 completed orders；
- 其中 4 个未被本地数据库识别的历史订单登记为 informational 外部订单；
- 当前没有外部开放订单，blocking difference 为 0；
- 当前连接会话的交易就绪状态为 `healthy`。
- DuckDB 自动升级到 schema version 6；
- `order.preview` 返回 reconciliation 和 close-only 判定；
- `Degraded` 只允许方向严格减少持仓的订单；
- 平仓数量不得大于当前持仓，禁止穿过零点建立反向持仓；
- 持仓快照必须晚于当前 IBKR session 的连接时间；
- 差异支持 acknowledged、说明和处置时间审计；
- acknowledge 不会解除 blocking gate，必须重新对账；
- paper 历史 informational 差异确认流程验收通过，未发送订单。

当前订单身份模型为：

```text
内部 OrderId
+ connection session ID
+ IBKR client ID
+ IBKR order ID
+ IBKR perm ID
+ execution ID
```

### 剩余工作

- 本地缺失成交修复；
- 对账差异的自动修复和受控忽略规则；
- completed order 的时区时间解析为 UTC；

## 12. 阶段 9：实时行情服务

**状态：已完成（首版）**

### 已完成内容

- bid/ask/last；
- 实时 tick；
- bid/ask/last size、OHLC、volume 等标准 tick 的通用保存；
- 实时、延迟等 MarketDataType 保存；
- persistent subscription 定义；
- daemon 重启恢复订阅；
- 自动恢复订阅；
- 单合约独立 subscribe/unsubscribe；
- IBKR delayed market-data fallback；
- 订阅状态：subscribing、awaiting_data、active、failed；
- 订阅错误持久化并通过 quote RPC 返回；
- 失败后 15 秒受控重试；
- 最新 tick 写入 DuckDB `market_ticks_current`；
- 有界事件队列和串行持久化。
- 行情健康状态区分 fresh、stale、missing；
- `max_market_data_age_seconds` 可配置，默认 30 秒；
- 新开仓要求行情 fresh；
- 严格减少当前持仓的平仓订单允许绕过行情故障；
- 成交价 tick 聚合为一分钟 OHLC；
- 新分钟首个成交到达时自动 final 前一分钟 Bar；
- 分钟 Bar 写入 `market_minute_bars`。

### RPC/CLI

```text
market_data.subscribe
market_data.unsubscribe
market_data.subscriptions
market_data.quote
market_data.health
market_data.bars
```

```bash
quant market-data subscribe ...
quant market-data unsubscribe --conid <CONID>
quant market-data subscriptions
quant market-data quote --conid <CONID>
quant market-data health --conid <CONID>
quant market-data bars --conid <CONID> --limit 100
```

### Paper 验收结果

- DuckDB 自动升级到 schema version 8；
- SPY 订阅定义成功持久化；
- daemon 重启后自动恢复 SPY 订阅；
- Gateway 返回错误 10197（存在竞争的 live session）；
- 错误内容和 failed 状态可由 quote CLI 查询；
- 后台按 15 秒间隔重试，未形成忙循环；
- DuckDB 自动升级到 schema version 9；
- 10197 条件下行情健康状态正确显示为 missing/failed；
- 健康对账会话中，SPY 新开仓预览因行情 missing 被门控拒绝；
- 同一预览确认零持仓不被误判为平仓；
- 分钟 Bar 聚合、OHLC、tick count 和 final 边界测试通过；
- 未执行交易。

### 剩余工作

- market data snapshot；
- 实时 5 秒 bar；
- 订阅引用计数；
- SMART 与 OVERNIGHT 行情路由；
- 高频行情合并；
- 实时行情写入 Parquet；
- RPC 长轮询或 WebSocket 事件。

### 完成标准

- 多个策略订阅同一合约只产生一份 IBKR 上游订阅；
- 重连后自动恢复；
- 行情过期时风险引擎禁止开仓；
- final bar 不静默丢失。

## 13. 阶段 10：完整风险引擎与交易就绪门控

**状态：已完成（首版）**

### 已完成

- 基础交易开关；
- 账户、证券类型、数量和名义价值检查；
- 幂等检查；
- 显式确认。
- 行情缺失和陈旧门控；
- 对账或行情降级时仅允许严格平仓；
- 单证券投影持仓上限；
- 组合 gross exposure 上限；
- 组合 net exposure 绝对值上限；
- 最大活跃订单数；
- 每分钟订单意图速率限制；
- 每日亏损熔断；
- 委托价格相对最新行情的最大偏离；
- 账户 PnL 和持仓快照陈旧门控；
- `order.preview` 返回完整组合风险指标；
- 基础风险和组合风险分别写入 `risk_decisions`；
- 严格平仓绕过持仓、敞口和日亏损限制，但不绕过速率限制。

### Paper 验收结果

- 账户 PnL 和持仓快照在当前连接会话内保持新鲜；
- SPY 零持仓的投影持仓为 1、gross/net exposure 为 100；
- 历史 `Unknown` 订单不会被误计为活跃订单；
- 行情缺失仍在组合风险通过后由交易就绪门控阻止开仓；
- 最大持仓、日亏损旁路平仓及价格偏离边界测试通过；
- 未执行交易。

### 剩余工作

- 最大回撤熔断；
- regular/extended/overnight 时段检查；
- 策略级额度；
- `pause_strategies`；
- `reject_new_orders`；
- `cancel_open_orders`；
- 人工批准模式；
- 对账未完成禁止开仓；
- IBKR 连接降级禁止开仓。

## 14. 阶段 11：策略运行器

**状态：已完成（信号、共享策略核心与 paper 执行）**

### 已完成内容

- schema 11：`strategies` 和 `strategy_evaluations`；
- 内置 `moving_average_cross` 策略及参数校验；
- 只消费 `final` 一分钟 Bar，避免读取未闭合 Bar；
- 以 `last_evaluated_bar` 保证同一策略、同一 Bar 只计算一次；
- buy、sell、hold 计算值、前值和时间全部持久化；
- daemon 内 5 秒调度，单次故障不会终止主进程；
- stopped、running、paused 状态持久化，重启后 running 策略自动恢复；
- 创建、列表、启动、暂停、停止及信号查询 RPC/CLI；
- 默认创建为 stopped，策略只产出信号，不直接提交订单；
- 单元测试覆盖参数、交叉信号和幂等游标。

```text
quant strategy create-ma --name spy-ma --conid 756733 \
  --short-window 5 --long-window 20
quant strategy list
quant strategy start <STRATEGY_ID>
quant strategy pause <STRATEGY_ID>
quant strategy stop <STRATEGY_ID>
quant strategy signals <STRATEGY_ID> --limit 100
```

### 尚未实现的更通用事件运行时

- `StrategyContext`；
- 行情、定时器、订单和成交事件；
- 每个策略独立任务；
- handler timeout；
- panic 隔离；
- StrategyRun ID；
- 代码版本和参数快照；
- 策略订单归属；
- paper/live 使用同一策略接口。

### 代码策略扩展（已完成）

- schema 16 为每次策略计算增加通用 `output_json` 审计字段；
- `src/strategy.rs` 定义确定性、无副作用的 `Strategy` trait；
- `StrategyBar`、`StrategySignal` 和 `StrategyOutput` 统一策略输入输出；
- 编译期 registry 负责从 kind 和 JSON config 构造策略；
- 实时运行器和回测引擎调用同一个 `Strategy::evaluate()`；
- `strategy kinds` 列出当前二进制已注册策略；
- `strategy create --kind --config-json` 创建任意注册策略；
- `backtest run-strategy --kind --config-json` 回测同一份策略代码；
- 当前注册五种策略：`moving_average_cross`、`moving_average_cross_5s`、
  `moving_average_cross_v2`、`close_threshold` 和 `paper_round_trip`；
- 完整开发说明见 [STRATEGIES.md](STRATEGIES.md)；
- 通用策略完成创建、启动、停止及本地 Parquet 回测验收。

### 策略执行层（已完成）

- schema 17：`strategy_execution_configs` 与 `strategy_execution_actions`；
- 每个 execution config 默认 disabled、强制 `paper_only=true`；
- buy/sell 分别调整到配置的目标仓位；支持显式空头目标和多腿组合；
- 同一账户、同一合约存在活动订单时跳过，不重复下单；
- config 启用时间以前的历史信号不会被执行；
- `(evaluation_id)` 和策略信号幂等键防止重复 action/订单；
- processing、submitted、rejected、failed、skipped 全程持久化；
- processing 期间崩溃会转为 failed 并要求人工核对，不自动重发未知结果订单；
- 后台 worker 仅在 paper 且 `trading_enabled=true` 时运行；
- 自动订单复用标准 `order.submit` JSON-RPC，继续经过全部风险、行情、对账和
  紧急停止门控；
- live 自动策略执行硬性禁止；
- 提供 configure、enable、disable、list、actions RPC/CLI；
- 单元测试覆盖目标仓位数量和同一信号仅认领一次。

### 首个示例策略

建议实现一个只用于验证系统的低频策略：

- 单品种；
- 只使用 final bar；
- 固定小仓位；
- 最大一笔开放订单；
- paper only；
- 默认不自动启动。

## 15. 阶段 12：回测与模拟撮合

**状态：已完成（首版长仓均线策略）**

### 已完成内容

- Parquet 数据读取；
- 确定性事件时钟；
- 下一 bar 成交规则；
- 滑点；
- 手续费；
- 资金和持仓；
- 权益曲线；
- 收益率、波动率、最大回撤和换手率；
- 回测参数、固定 seed 和数据文件 ID 快照；
- 固定随机 seed；
- 防未来函数；
- 回测 run、成交、权益和指标写入 DuckDB；
- schema 12：`backtest_runs`、`backtest_trades`、`backtest_equity`；
- `backtest.run`、`backtest.list` RPC/CLI；
- 单元测试验证信号只能在下一根 Bar 开盘成交；
- 使用本地 AAPL 日线 Parquet 完成端到端验收。

### 后续撮合扩展

- 限价、止损、部分成交与 OHLC 歧义保守规则；
- regular、extended、overnight 交易时段模型；
- 多资产保证金和做空；
- 策略代码版本快照以及权益曲线 Parquet 导出。

## 16. 阶段 13：数据治理与批量分析

**状态：已完成（首版）**

### 已完成

- DuckDB；
- Parquet；
- dataset manifest；
- 基础 SQL 数据写入；
- 普通 Rust struct 内存模型。

### 剩余工作

- active dataset DuckDB views；
- 参数化分析查询；
- OHLCV 重采样；
- 收益率、波动率和滚动指标；
- 数据 gap report；
- dataset snapshot；
- compaction；
- retention；
- schema migration；
- checksum；
- 受控只读 SQL RPC；
- 查询超时和行数限制；
- 必要时才引入 Polars；
- Arrow 仅限存储和分析边界。

### 首版完成增量

- schema 13 为 dataset manifest 增加文件校验和；
- `data.verify` 对每个 active Parquet 检查存在性、字节数和校验和；
- 旧文件首次校验时安全回填校验和；
- `dataset_snapshots` 固化 active file ID 集合，回测也绑定数据文件 ID；
- `data snapshot create/list` 提供可复现实验的数据版本；
- 历史任务具备切片、重试、游标、取消与 raw gap report；
- DuckDB SQL 承担 Parquet 读取、回测批量输入和统计分析；
- 首版数据量无需 Polars/Arrow，继续遵循“必要时再引入”的设计约束。

compaction、retention、任意只读 SQL 和 Polars 属于容量扩展。首版选择参数化 RPC，
避免开放可阻塞 daemon 或读取内部敏感表的任意 SQL。

## 17. 阶段 14：监控、备份和生产化部署

**状态：已完成（首版）**

### 已完成

- tracing 日志；
- JSON/pretty 格式；
- correlation 基础结构；
- daemon 状态；
- 进程锁；
- 优雅停止；
- 配置校验；
- RPC 安全默认监听 loopback，监听地址与浏览器 Origin 可显式配置。

### 剩余工作

- 文件日志轮转；
- 日志保留策略；
- metrics；
- channel 深度；
- IBKR 请求延迟和错误率；
- 数据延迟；
- 订单和风险指标；
- 磁盘空间监控；
- DuckDB 定期备份；
- Parquet 和配置联合备份；
- 恢复演练；
- staging/quarantine 清理；
- `screen` 后台守护脚本；
- 自动重启；
- 健康检查；
- 告警；
- migration checksum；
- 关键任务 supervisor；
- daemon 崩溃故障注入。

### 首版完成增量

- `system.health` 返回 daemon、队列、策略、行情失败数及数据库/湖/staging 大小；
- `backup.create` 先执行 DuckDB `CHECKPOINT`，再联合复制数据库和 active Parquet；
- 每份备份包含 schema version、文件 ID 和校验和 manifest；
- `backup.list` 提供备份审计；
- staging 容量进入健康检查，历史写入仍使用 staging 后原子 rename；
- 提供 `screen-start.sh`、`screen-run.sh`、`screen-status.sh` 和
  `screen-stop.sh`，包含自动重启、日志留存、状态检查与优雅停止；
- daemon 保持进程锁、优雅停止、loopback RPC 与 tracing 结构化日志；
- 实际创建 schema 14 联合备份并验证 manifest 中两个 Parquet 文件。

外部 metrics exporter、集中告警和日志轮转属于部署环境集成，不嵌入个人单进程首版。

### 可靠性修复增量（2026-07-28）

面向无 systemd、`screen` 后台运行的场景完成以下修复：

- 5 个关键后台任务（broker 事件持久化、策略评估、策略执行、backfill、自动对账）
  纳入监督：panic 或意外退出会触发优雅停机并以非零码退出，不再静默丢失持久化；
- Storage 互斥锁中毒自动恢复并记录日志，消除级联 panic 导致的"活死"进程；
- RPC 服务端对请求读取和响应写入强制超时；并发额度占满或 accept 出错时
  停机信号仍然生效；socket 目录权限收紧为 `0700`；
- 订单 ack 超时或响应流中断时 intent 标记为 `unknown`（错误码 `-32026`），
  不再误标 rejected；策略执行 action 在结果不确定时标记 failed 要求人工核对；
- `order.submit` 的门控拒绝（紧急停止、live 未批准、账户、就绪）同样持久化
  intent 与 risk decision；全部风控检查与 intent 写入在单一存储临界区完成，
  消除并发提交挤过共享限额的竞态；
- 市价单名义额检查优先使用本地行情价，弱化自报 estimated price 的绕过空间；
- 数据库 schema 版本高于程序支持版本时拒绝启动；
- 重复 backfill 时被完全覆盖的旧 Parquet 文件在同一事务中置 inactive；
- 回测失败记录为 `failed` run，不再遗留 `running` 僵尸记录；
- 策略评估循环单个策略失败被隔离并写入该策略的 `last_error`；
- IBKR 重连退避加入 jitter；连接丢失立即发布 `Reconnecting` 状态；
- 配置加载警告（live 交易开启、环境变量覆盖 `trading_enabled`）在 telemetry
  初始化后输出，不再丢失。

## 18. 阶段 15：实盘准入与渐进发布

**状态：准入门控已完成，实盘仍未批准**

### 前置条件

以下条件全部满足前，不允许启用无人值守实盘：

- 完成连接 session ID 和 perm ID 对账；
- 完成开放订单、completed orders、execution 和 commission 恢复；
- 对账差异能阻止开仓；
- 完成实时行情和陈旧检查；
- 完成完整风险门控；
- 完成 paper 长时间运行测试；
- 完成断网、Gateway 重启和 daemon 崩溃测试；
- 完成备份恢复；
- 所有订单类型有状态机测试；
- 没有无法解释的重复订单；
- 紧急停止经过人工演练。

### 渐进步骤

1. Paper 手工 CLI；
2. Paper 自动策略；
3. Paper 连续运行至少数周；
4. Live 只读；
5. Live 只允许撤单和平仓；
6. Live 单一白名单 ETF、最小数量；
7. Live 低频自动策略；
8. 逐步扩大白名单和额度。

实盘配置默认必须保持：

```toml
[risk]
trading_enabled = false
```

只有在明确的部署配置中临时或显式开启，不能修改为不安全的内置默认值。

### 已实现的准入控制

- schema 14 `trading_control` 默认 `live_approved = false`；
- `emergency_stop` 持久化并同时启用 `reject_new_orders`、`pause_strategies`；
- `reject_new_orders` 在 broker submit 之前硬阻断；
- `pause_strategies` 在策略调度器读取 Bar 之前硬阻断；
- live 环境还要求显式批准且 conid 位于白名单；
- approve、revoke、normal 恢复全部要求 CLI 显式确认和 operator note；
- 实际完成 emergency stop / status / reset 演练，未发送订单；
- 当前 paper 配置 `trading_enabled = false`，`live_approved = false`。

因此代码层准入阶段已经完成，但“paper 连续运行至少数周”等时间性证据不能由一次
开发会话伪造。未积累这些证据前，平台会继续拒绝实盘自动交易。

## 19. 阶段 16：多资产与高级订单扩展

**状态：明确排除在首个稳定版之外**

### 资产类型

- 期权；
- 期货；
- 外汇；
- 指数和指数期权；
- 债券；
- 加密货币；
- 组合合约。

### 高级订单

- 止损；
- 止损限价；
- 追踪止损；
- bracket order；
- OCA；
- parent/child；
- 做空专用指令；
- combo order；
- algo order。

这些功能只能在股票/ETF 基础交易、对账、风险和恢复机制稳定后开始。

## 20. 当前可用功能清单

当前可以安全用于 paper 验证的主要命令（完整参数以 `quant --help` 和各子命令
`--help` 为准）：

```bash
quant daemon
quant status
quant version
quant shutdown

quant ibkr status
quant ibkr connect
quant ibkr disconnect

quant account managed
quant account summary
quant account pnl
quant positions

quant instrument search AAPL
quant instrument list

quant data backfill ...
quant data jobs
quant data cancel <JOB_ID>
quant data coverage ...
quant data verify
quant data snapshot create/list ...
quant market-data subscribe ...
quant market-data subscriptions
quant market-data quote --conid <CONID>
quant market-data health/bars ...
quant market-data unsubscribe --conid <CONID>

quant strategy kinds/create/create-ma/list/start/pause/stop/signals ...
quant strategy execution configure/configure-portfolio/enable/disable/list/actions ...
quant performance report/snapshots ...
quant monitor metrics/alerts/acknowledge ...
quant fx set/list ...
quant calendar add/list/status ...
quant backtest run/run-strategy/list ...

quant order preview ...
quant order submit ... --confirm
quant order cancel ... --confirm
quant order list

quant executions
quant reconcile
quant health
quant backup create/list
quant safety status/set/live-approve/live-revoke ...
```

当前支持的交易范围：

- IBKR paper；
- 当前风险与执行主路径面向股票和 ETF，即 `STK`；
- buy/sell；
- market/limit；
- 人工订单可按参数选择常规/盘前盘后及路由；
- 策略自动执行默认使用市价单；允许盘前盘后时使用最新 Bid/Ask 定价的限价单并
  设置 `outside_rth=true`；
- 整股数量。

## 21. 当前已知风险和技术债

按优先级排列：

1. IBKR Gateway 当前未监听 4002，在线验收需 Gateway 恢复后继续；
2. paper 自动策略尚未完成数周连续运行证据，因此 live approval 保持关闭；
3. 交易日历尚未用于 coverage gap 过滤；
4. 撤单、部分成交和 execution correction 仍需更长时间 paper 故障注入；
5. 存储使用进程内互斥串行写入（已具备锁中毒恢复与任务监督），回测、备份等
   长操作仍会阻塞其他请求；长查询未来可迁移到 StorageWriter actor；
6. 策略运行器会产出信号；只有单独配置、显式授权且通过全部门控的 paper 策略才会
   自动执行；
7. 首版主路径仅支持股票/ETF 市价、限价单；多资产与高级订单明确延期；
8. IBKR 请求仍无 token bucket pacing（backfill 依靠切片加 2 秒轮询节流）；
9. Parquet staging 残留临时文件与无 manifest 孤儿文件尚无启动扫描/quarantine。

## 22. 推荐的后续交付顺序

首个稳定版开发阶段已经结束。后续发布顺序：

1. 恢复 IB Gateway 后重新跑连接、对账和历史任务 smoke test；
2. Paper 自动策略持续运行并收集数周证据；
3. 完成断网、Gateway 重启、daemon 崩溃及备份恢复演练；
4. 仅在审核证据后批准单一白名单 ETF 和最小额度 live；
5. 最后才考虑多资产与高级订单。

## 23. 总体完成度判断

股票/ETF paper 首个稳定版的开发工作已经完成；live 发布仍受持久化门控和长期
paper 证据约束，不能把“代码完成”等同于“已经适合无人值守实盘”。

可按以下方式理解：

```text
架构设计                 已完成
daemon / RPC / CLI       已完成
IBKR 连接                已完成
历史行情 / Parquet       已完成（首版）
人工 paper 交易          已完成
成交和手续费             已完成
重启对账                 已完成（首版）
实时行情                 已完成（权限失败时安全降级）
策略信号运行器           已完成
确定性回测               已完成
风险、备份和恢复交付物   已完成（首版）
实盘准入代码门控         已完成，默认关闭
无人值守实盘             等待长期 paper 证据
```

当前最重要的目标不是增加更多品种，而是运行长期 paper soak test，持续验证断线、
重启、重复消息、部分成交和外部操作下的准确性、可恢复性与可审计性。

## 24. 阶段 17：长期策略运行闭环

**状态：已完成（paper 首版）**

本阶段补齐从“策略能够自动发单”到“能够长期观察、解释并约束运行结果”的闭环：

- 数据库升级至 schema 19，新增 FX 汇率、交易 session、策略绩效快照、持久化告警、
  组合执行配置和 action leg 审计表；
- 绩效按 strategy action、order intent、execution 和 commission 归因，输出毛 PnL、
  净 PnL、换手率、胜率、最大回撤、Sharpe、Sortino、每日权益和可选基准收益；
- monitoring worker 周期保存启用策略的绩效快照，并检测 IBKR 未就绪、对账异常、
  行情失败/延迟、Unknown 订单、失败 action 和快照错误；
- 风控统一使用配置的基础币种。非基础币种风险敞口必须通过新鲜 FX 汇率折算，
  IBKR Account Summary 的 ExchangeRate 可自动写入；
- 支持显式 UTC 交易 session。配置过某交易所日历后，闭市、休市或缺失当日 session
  会阻止该交易所的策略自动执行；
- 自动执行严禁使用 IBKR delayed tick，只有实时 bid/ask 才可定价；人工严格减仓
  仍保留故障降级通道；
- 单腿目标仓位增加受控做空配置；组合策略支持多腿目标仓位、全部腿预检和逐腿提交，
  每腿状态独立持久化。由于 IBKR 多张独立证券订单不具备原子事务语义，任一腿结果
  不确定时停止后续提交并要求对账，不盲目反向补偿；
- 后台部署只采用 GNU `screen`：启动脚本负责 detach、日志和异常重启，停止脚本先
  请求 daemon 优雅退出并写停止标志，避免守护循环再次拉起；不再提供 systemd 单元。

对应 CLI：

```bash
quant performance report/snapshots ...
quant monitor metrics/alerts/acknowledge ...
quant fx set/list ...
quant calendar add/list/status ...
quant strategy execution configure --allow-short ...
quant strategy execution configure-portfolio ...
deploy/screen-start.sh config/paper.toml
deploy/screen-status.sh config/paper.toml
deploy/screen-stop.sh config/paper.toml
```

这一阶段完成的是可验证的 paper 运行能力，并不自动构成实盘盈利证明。策略盈利能力
仍需依靠未参与参数选择的样本外数据、合理成本模型和持续数周的 paper forward test
来评估。

## 25. 阶段 18：高频 Bar、V2 策略与订单诊断

**状态：已完成（paper 诊断增量）**

- schema 20 增加 5 秒 Bar；实时成交 Tick 同时聚合 1 分钟和 5 秒 OHLC；
- 增加 `moving_average_cross_5s`，以及支持 1m/5s、SMA/EMA、均线 gap、确认、
  冷却、ATR 和趋势过滤的 `moving_average_cross_v2`；
- schema 21 为订单保存 `remaining_quantity`、`last_fill_price`、`why_held` 和
  `market_cap_price`；
- 新增 `broker_order_events`，审计 IBKR open/completed order 的状态、拒绝原因、
  警告文本和完成状态；
- 风控认可当前会话已完成的空持仓快照，避免完全空仓账户因没有持仓行而被错误判定
  为“position data is missing”；同步中和真正过期的快照仍禁止开仓；
- Web 的订单与策略状态页面展示这些诊断字段，便于区分未成交、部分成交、broker
  已无活动订单和明确拒绝；
- 实时绩效佣金继续以 IBKR `CommissionReport` 为准，非基础币种在报告生成时按当前
  新鲜 FX 换算。

仍未解决的成本建模限制：

- 回测 `commission_per_order` 只是每笔固定金额，不识别市场费率、最低收费、税费或
  平台费；
- `min_gap_percent` 是指标过滤条件，不代表预期收益覆盖费用；
- 本阶段结束时自动执行尚无交易成本门槛；该限制已由下一阶段的数据库费用模型解决。
  5 秒等高换手策略仍需要长周期 paper 验证。

## 26. 阶段 19：数据库费用模型与成本感知执行

**状态：已完成（paper）**

- schema 22 增加数据库费用模型、策略成本控制和 action 成本审计字段；
- Web“交易成本”页面支持创建和修改固定费、比例费、最低费、卖出税费、点差和
  滑点模型，并绑定到策略；
- 执行前把完整往返费用折算为 bps，并乘配置安全倍数与均线指标差强度比较；
- 固定费自动惩罚小额订单，比例费随名义金额计算，两者可同时配置；
- 使用策略历史实际 `CommissionReport` 有效费率 P90 与配置模型取更保守值；
- 成本不足的 action 记为 `skipped`，保留名义金额、预计成本、信号强度和门槛；
- 最新绩效快照达到最少交易数后，佣金/毛利润超过上限会自动关闭该策略执行配置。

schema 23 继续为费用模型增加买入和卖出每股费用，单边费用统一按
`max(最低费, 每笔固定费 + 数量×每股费 + 名义金额×比例费率)` 计算。

schema 24 为策略执行配置增加 `outside_rth`；常规时段模式继续使用市价单，盘前
盘后模式使用最新 Bid/Ask 作为限价并把 `outside_rth=true` 传给 IBKR。

schema 25 增加可保存单日多个区间的交易日历缓存。自动执行按需读取 IBKR
`ContractDetails`，分别缓存 `liquidHours` 和 `tradingHours`，并按合约时区转换为
UTC；正常和盘前盘后订单都必须命中对应时段。
