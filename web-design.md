# Web 前端与 RPC Workspace 设计

> 本文保留 Web/RPC 重构的设计依据，并记录当前实现。实际命令和操作流程以
> [README.md](README.md) 为准，策略行为以 [STRATEGIES.md](STRATEGIES.md) 为准。

## 0. 当前实现状态

重构已经完成：根后端、`rpc-types` 与 `web` 是同一个 Cargo workspace；daemon
使用 `jsonrpsee` 同时提供 HTTP 和 WebSocket JSON-RPC，CLI 与 Yew/WASM 客户端共享
`quant-rpc-types`。Unix Domain Socket 和自定义换行 framing 已不再使用。

Web 当前包含总览、证券搜索、策略、策略状态、策略绩效、回测、下载任务、交易成本、
交易日历、均线策略向导、均值回归向导、Paper 验证、订单与成交、运行维护、实时日志、
RPC 工具和 RPC 设置。页面默认每
5 秒刷新；RPC 工具的方法表来自 `quant_rpc_types::ALL_METHODS`，因此新增 RPC 时
应同时更新共享清单。

监听地址不是代码硬编码的 loopback：`rpc.http_listen` 与 `web.listen` 决定实际
网络边界。示例配置使用 loopback；若配置为 `0.0.0.0`，必须依靠可信局域网、防火墙
或带 TLS、认证和访问控制的反向代理。`allowed_web_origin = "*"` 只影响浏览器
Origin 校验，不会把外部监听自动变安全。

## 1. 目标

为现有个人量化交易平台提供一个位于 `web/` 的 Yew WebAssembly 前端，使用
[`isosphere/yew-bootstrap`](https://github.com/isosphere/yew-bootstrap) 提供
Bootstrap 5 组件，通过 JSON-RPC 操作 daemon。

本次重构同时完成：

- 将根项目、Web 前端和共享 RPC 类型纳入同一个 Cargo workspace；
- 将目前散落在 `src/rpc.rs`、`src/main.rs` 和领域模块中的 RPC 请求、响应与公共
  DTO 抽到独立的 `rpc-types` crate；
- 使用 `jsonrpsee` 统一服务端、CLI client、WASM client 和 RPC codegen；
- 删除 Unix Domain Socket RPC，CLI 和 Web 统一使用监听地址可配置的 HTTP/WebSocket
  JSON-RPC；
- 所有查询和变更都经过现有 daemon 的 RPC、风控、审计和幂等链路，前端不直接访问
  DuckDB、Parquet 或 IB Gateway；
- 对下单、启用自动执行、紧急停止、live approval 等高风险操作提供明确确认流程。

首版是本机单用户控制台，不设计多租户、远程公网部署或移动端应用。

## 2. 重构前现状与关键约束

重构前 RPC 是手写的 JSON-RPC 2.0：

- Unix Socket：`data/run/quant.sock`；
- 请求和响应以换行符分隔；
- 每个连接处理一个请求；
- CLI 和 daemon 位于同一个二进制；
- 参数结构体大多是 `src/rpc.rs` 的私有类型；
- 许多响应通过 `serde_json::json!` 临时构造，前端无法在编译期检查字段；
- RPC dispatch 同时依赖 IBKR handle、Storage、风险配置和取消令牌。

重构后不保留 Unix Socket。daemon 在配置的 TCP Socket 上提供 `jsonrpsee`
HTTP/WebSocket server：

```text
CLI ── jsonrpsee HttpClient ─┐
                            ├── jsonrpsee RpcModule ── 服务、风控、存储、IBKR
Web ─ jsonrpsee WasmClient ─┘
```

`jsonrpsee` 是唯一 RPC 实现，不新增绕过 RPC 的 REST 业务接口。CLI 使用 HTTP，
浏览器受 WASM 限制在同一 TCP listener 上使用 WebSocket；两者都使用 JSON-RPC
2.0，而不是自定义裸 TCP framing。
现有 `unix_socket` 配置、UDS listener、换行 framing、socket 文件权限和 stale
socket 清理代码均直接删除。

`yew-bootstrap` 是面向 Yew 的 Bootstrap 5 组件库，并按语义化版本发布。实现时
优先使用 crates.io 正式版本，把 `yew-bootstrap`、Yew 和 Bootstrap CSS 锁到经过
验证的兼容版本；不能使用 `*`、浮动 Git 分支或未经验证的最新版组合。Bootstrap
CSS 作为项目静态资源随 `web/dist` 一起交付，不从 CDN 动态加载，从而满足离线使用
和严格 CSP。

## 3. Workspace 目录

保留根 crate，避免无意义搬迁全部后端源码：

```text
simple-quantitative-trader/
├── Cargo.toml                  # workspace + 后端 package
├── Cargo.lock                  # workspace 唯一 lockfile
├── src/                        # daemon 与 CLI
├── rpc-types/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── api.rs             # jsonrpsee QuantRpc trait
│       ├── domain.rs
│       └── methods/
│           ├── system.rs
│           ├── ibkr.rs
│           ├── account.rs
│           ├── instrument.rs
│           ├── market_data.rs
│           ├── strategy.rs
│           ├── execution.rs
│           ├── performance.rs
│           ├── order.rs
│           ├── reconcile.rs
│           ├── monitoring.rs
│           ├── data.rs
│           └── safety.rs
├── web/
│   ├── Cargo.toml
│   ├── Trunk.toml
│   ├── index.html
│   ├── assets/
│   │   ├── app.css
│   │   └── icons/
│   ├── src/
│   │   ├── main.rs
│   │   ├── app.rs
│   │   ├── route.rs
│   │   ├── api/
│   │   │   ├── mod.rs
│   │   │   ├── client.rs
│   │   │   └── polling.rs
│   │   ├── components/
│   │   ├── pages/
│   │   └── state/
│   └── tests/
└── web-design.md
```

根 `Cargo.toml`：

```toml
[workspace]
members = [".", "rpc-types", "web"]
resolver = "3"

[package]
name = "simple-quantitative-trader"
# 保留现有 package 配置

[dependencies]
quant-rpc-types = { path = "rpc-types" }
jsonrpsee = { version = "=0.26.0", features = ["server", "http-client"] }
```

`web` 也依赖同一个 `quant-rpc-types`。根 crate 可以继续同时产出 daemon/CLI
二进制；不要求为了 workspace 把它拆成更多 crate。

## 4. `rpc-types` crate

### 4.1 职责

`rpc-types` 是基于 `jsonrpsee` codegen 的纯协议 crate：

- `#[rpc(client, server)]` API trait 和 RPC method 名称；
- params 和 result DTO；
- 对外可见的枚举及状态；
- RPC error code 常量；
- 服务端和客户端都能使用的序列化测试。

它不能依赖：

- DuckDB、Tokio、ibapi；
- daemon 的 Storage、Handle 或 Config；
- 浏览器 API；
- CLI clap 类型；
- 任何业务执行逻辑。

业务依赖仅包含 `serde`、`serde_json`、`chrono`、`uuid` 和可选的 `thiserror`，
另加 `jsonrpsee` 的 `macros`、`client-core`、`server-core` feature。此 crate 不
启用具体 HTTP/WS transport。`chrono` 与 `uuid` 必须启用 serde，并验证
`wasm32-unknown-unknown` 编译。

### 4.2 类型化 API

JSON-RPC envelope、request ID、response 和标准错误交给 `jsonrpsee`。项目只定义
业务 DTO、稳定错误码以及一个共享 API trait：

```rust
use jsonrpsee::{core::RpcResult, proc_macros::rpc};

#[rpc(client, server)]
pub trait QuantRpc {
    #[method(name = "system.status")]
    async fn system_status(&self) -> RpcResult<SystemStatus>;

    #[method(name = "strategy.create")]
    async fn strategy_create(
        &self,
        params: StrategyCreateParams,
    ) -> RpcResult<StrategyCreateResult>;
}
```

宏生成 `QuantRpcClient` 和 `QuantRpcServer`。后端实现 server trait 并注册为
`RpcModule`；CLI 的 `HttpClient` 和前端的 `WasmClient` 都调用生成的
`QuantRpcClient`，不再手写 method 字符串或 envelope。

当前协议使用 object params。codegen 的实际参数编码必须在 compatibility spike 中
用 JSON fixture 确认。如果不能原样保持，则将新接口明确标记为 RPC v2，并让
daemon、CLI 和 Web 在同一版本中原子迁移；不得在相同 `rpc_version` 下静默改变
params 形状。

### 4.3 DTO 迁移原则

第一轮覆盖 `system.version` 返回的全部 capability，优先消除 `Value`：

- `SystemStatus`、`SystemHealth`、`VersionInfo`；
- `ConnectionStatus`、`ReconciliationHealth`；
- `AccountSummaryEntry`、`AccountPnl`、`Position`；
- `Instrument`、`ContractCandidate`；
- `MarketDataSubscription`、`Quote`、`MarketBar`；
- `Strategy`、`StrategyEvaluation`、`StrategyExecutionConfig/Action/Leg`；
- `PerformanceReport`、`PerformanceSnapshot`；
- `OrderPreview`、`OrderIntent`、`Execution`；
- `MonitoringMetrics`、`MonitoringAlert`；
- `DataJob`、`CoverageReport`、`DatasetSnapshot`、`BackupInfo`；
- `FxRate`、`MarketSession`、`TradingControl`。

时间字段统一为 `DateTime<Utc>`，ID 使用 `Uuid`，IBKR conid 保留 `i32`，broker
order ID 保留 `i32`，数据库 bigint 使用 `i64`。金额和数量首版继续与后端一致使用
`f64`；后续若引入 decimal，必须通过新的 RPC major version 迁移。

所有枚举使用稳定的 `snake_case` serde 表示。前端遇到未知枚举值时不能导致整个页面
不可用，因此协议演进优先新增可选字段；新增 enum variant 时同步升级三个 workspace
成员。

### 4.4 版本与兼容

`system.version` 返回：

```rust
pub struct VersionInfo {
    pub application_version: String,
    pub rpc_version: u32,
    pub capabilities: Vec<String>,
}
```

Web 启动先请求版本：

- RPC major 不兼容：显示阻断页；
- capability 缺失：隐藏或禁用对应功能；
- 可选字段未知：忽略；
- 方法不存在：显示“后端版本不支持”，不无限重试。

## 5. 后端 RPC 重构

### 5.1 `jsonrpsee` 服务实现

用共享 crate 的方法名和参数 DTO 注册 `RpcModule`，业务处理继续委托给现有
service/dispatch 边界：

```rust
pub struct QuantRpcService {
    // status、ibkr、storage、risk 等依赖
}

impl QuantRpcServer for QuantRpcService {
    // 每个方法调用现有业务服务、风控和存储
}
```

jsonrpsee server 负责 envelope、方法注册、参数反序列化、response 和连接/body
限制；Tower middleware 负责 Origin 校验。后续可在不改变 wire contract 的前提下，
把动态注册逐步替换成 `#[rpc]` 生成的 server trait。

订单提交期间不能因 HTTP client 断开而取消业务 future；沿用现有原则：传输读写可以
超时，已经进入服务实现的变更操作必须运行至确定完成或记录为 unknown。

### 5.2 HTTP JSON-RPC

daemon 的 loopback HTTP RPC 始终启用，因为 CLI 和 Web 共用它；静态 Web UI 可以
单独关闭：

```toml
[rpc]
http_listen = "127.0.0.1:8787"
request_timeout_seconds = 30
max_request_bytes = 1048576
max_concurrent_requests = 32
allowed_web_origin = "http://127.0.0.1:8080"

[web]
enabled = true
listen = "127.0.0.1:8080"
static_dir = "web/dist"
```

路由：

```text
TCP 127.0.0.1:8787  jsonrpsee HTTP/WebSocket JSON-RPC 2.0
GET /                 web/dist/index.html（独立 loopback 静态服务器）
GET /assets/*         带 hash 的静态文件
GET /*                SPA history fallback
```

不为每个 method 创建 REST endpoint。RPC 和静态文件使用不同端口，
减少静态资源路由对交易 RPC 的影响。

开发模式由 Trunk 在 `127.0.0.1:8080` 提供前端，daemon 允许这个明确 origin。
安全默认值和示例配置使用 loopback；配置允许显式改为 `0.0.0.0`，此时必须通过可信
局域网、防火墙、SSH tunnel 或具备 TLS/身份认证的反向代理访问。

### 5.3 安全

- HTTP RPC 始终启用；默认配置使用 loopback，显式配置可开放到其他接口；
- 浏览器 WebSocket 默认严格校验精确的 `allowed_web_origin`；显式配置 `*` 时允许
  任意 Origin 并记录安全警告，不支持部分 glob；
- CLI 等原生客户端不发送 `Origin`；可达范围取决于 `rpc.http_listen` 和网络策略；
- 本地同权限进程属于信任边界；如需跨机器使用，必须另加 TLS 和身份认证代理；
- 设置 CSP、`X-Content-Type-Options: nosniff`、`frame-ancestors 'none'`；
- mutation 不使用 GET；
- 继续要求现有 `confirm`、idempotency key、paper/live 门控和风险检查；
- order idempotency key 由 Web 在用户打开确认对话框时生成 UUID，并在响应未知时保持
  原 key，禁止自动换 key 重试；
- `system.shutdown`、`safety.*`、`order.submit/cancel`、执行层 enable/disable
  必须使用二次确认对话框；
- live 操作显示红色环境标识，并要求用户重新输入确认短语；前端确认不能代替后端
  `confirm_live_risk`。

首版不在 Web 页面中展示或编辑完整 TOML，防止泄漏账户配置和错误地把浏览器当作
密钥管理器。

## 6. Yew 前端

### 6.1 技术栈

- Yew CSR；
- `yew-bootstrap`；
- `yew-router`；
- `jsonrpsee` `wasm-client`；
- `wasm-bindgen-futures`；
- `quant-rpc-types`；
- Trunk 构建；
- 普通 CSS variables 实现主题和响应式布局。

RPC WebSocket URL 可在“RPC 设置”页面修改，并以键
`quant-trader.rpc-endpoint` 保存在浏览器 LocalStorage。启动时优先读取该值，保存
后立即重建 RPC client 并刷新所有数据。这样静态 Web 页面可以和 daemon 分别部署；
跨机器访问应通过 SSH 隧道或带 TLS、身份认证的 WebSocket 反向代理完成。

不在首版引入 Node/npm 状态管理或大型图表框架。权益曲线可先用原生 SVG Yew
组件；需要复杂交互后再评估专用图表库。

依赖策略：

1. 建立最小 compatibility spike，渲染 `yew-bootstrap` Button、Modal 和 Navbar；
2. 验证 Yew CSR、Trunk、Bootstrap 5 CSS 和目标浏览器；
3. 优先选择兼容的 crates.io 正式版本，并由 workspace `Cargo.lock` 精确锁定；
4. 若正式版不能兼容当前 Yew，才使用精确 Git `rev`，并在本文记录原因；
5. Bootstrap CSS 固定版本并复制到 `web/assets/vendor/`，不依赖公网 CDN；
6. CI 使用 `--locked` 构建，禁止浮动 `yew-bootstrap = "*"` 或 Git branch。

### 6.2 信息架构

主导航：

1. **Dashboard**
   - daemon、paper/live、IBKR、对账、行情和告警状态；
   - 账户净值/PnL 摘要；
   - 当前持仓；
   - 运行策略和最近 action；
   - 最近绩效快照。
2. **Portfolio**
   - 账户摘要、PnL、持仓和敞口；
   - 币种及 FX 新鲜度；
   - 手动刷新。
3. **Market Data**
   - 合约搜索与选择；
   - 行情订阅、quote、fresh/delayed/failed 状态；
   - 最近 minute bars；
   - backfill job、精确请求范围进度、成功抓取 coverage 和数据校验；
   - 区分正在下载、排队中和等待 IBKR，显示队列位置、前方任务数及当前 worker 任务；
   - 重叠活动请求由后端合并，Web 显示新建、复用或扩展已有任务的结果；
   - 只有后端返回 `backtest_ready=true` 才允许回测，任意重叠文件不能代表完整覆盖。
4. **Strategies**
   - 创建、启动、暂停和停止；
   - signal 时间线；
   - 单腿/组合 execution 配置；
   - enable/disable 确认；
   - action 和每腿执行状态。
5. **Performance**
   - 策略选择；
   - 初始资金和 benchmark；
   - 权益/回撤曲线；
   - 净 PnL、Sharpe、Sortino、胜率和换手率；
   - 历史快照。
6. **Backtest**
   - 只选择已保存策略，证券、周期和交易时段由策略配置锁定；
   - 要求历史下载完整覆盖请求范围；
   - 展示策略绑定的数据库费用模型并校验币种，不再提供独立佣金/滑点输入；
   - 展示费用模型快照，以及佣金/税费、点差、滑点和总执行成本；
   - 长周期权益曲线由服务端限量均匀抽样，成交记录使用服务端分页，详情响应不得通过
     提高 RPC 大小上限来容纳全部 5 秒数据。
7. **Orders**
   - preview 表单；
   - 风险决策逐项展示；
   - submit 二次确认；
   - order、execution、commission 和 unknown 状态；
   - 撤单。
8. **Operations**
   - monitoring metrics 和 alerts；
   - reconcile 状态、差异和 acknowledge；
   - backup、数据快照、FX 和 UTC 交易 session；
   - emergency stop；
   - IBKR connect/disconnect。

首版不在 Web 中提供 `live_approve`。该操作继续只允许 CLI，降低浏览器误操作风险。
Web 可以只读显示当前 live approval 状态。

### 6.3 页面交互约定

- 顶部固定显示 `PAPER`/`LIVE` 环境芯片、IBKR 状态和 active alert 数；
- 页面初始加载使用 skeleton，局部刷新不清空已有数据；
- 查询失败显示可重试错误，不用空表伪装成功；
- UTC 为数据真值，同时在界面显示用户本地时间，tooltip 展示完整 UTC；
- 数量、币种、价格和百分比按字段语义格式化，不修改原始精度；
- 表格支持 loading、empty、error 三种明确状态；
- 表单先在浏览器做基本校验，daemon 仍是最终校验者；
- 所有 mutation 成功后只刷新相关 query；
- JSON-RPC error 显示稳定标题、message、code 和 correlation/request ID；
- unknown order 使用不可忽略的高优先级 banner，禁止“一键重试提交”。

### 6.4 状态与轮询

不在首版增加 WebSocket/SSE。使用可见页面条件轮询：

| 数据 | 前台间隔 | 页面隐藏 |
|---|---:|---|
| system/IBKR/reconcile/alerts | 5 秒 | 30 秒 |
| quote/position/order/action | 2 秒 | 暂停或 30 秒 |
| performance snapshot | 30 秒 | 暂停 |
| instrument、backup、calendar | 手动/变更后 | 不轮询 |

使用 `document.visibilityState` 降频。每个资源最多一个 in-flight 请求；慢请求不叠加。
route 离开时取消前端等待，但 mutation 已送达后仍通过相同 idempotency key 查询结果，
不假设 HTTP abort 等于后端取消。

全局状态仅保存：

- API base URL 和 session token；
- `VersionInfo`/capabilities；
- 当前环境和连接状态；
- active alert 数；
- UI 主题。

页面业务数据留在页面 model 中，避免一个巨大 store。

## 7. RPC 到页面映射

| 页面 | 查询 RPC | 变更 RPC |
|---|---|---|
| Dashboard | `system.*`, `account.*`, `portfolio.positions`, `monitor.*` | `ibkr.connect` |
| Market Data | `instrument.*`, `market_data.*`, `data.*` | subscribe、unsubscribe、backfill |
| Download Jobs | `data.jobs`（服务端分页及全局队列摘要） | `data.job.cancel` |
| Strategies | `strategy.*`, `strategy.execution.*` | create/start/pause/stop/configure/enable |
| Performance | `performance.*` | 无 |
| Backtest | `backtest.*`, `execution_cost.*`, `data.coverage` | `backtest.run`、创建下载任务 |
| Orders | `order.list`, `execution.list`, positions | preview/submit/cancel |
| Operations | reconcile、backup、FX、calendar、safety | acknowledge/create/set |

当前 `system.version.capabilities` 是前端功能开关的唯一来源，不根据版本字符串猜测功能。

## 8. 构建、运行与交付

开发：

```bash
cargo check --workspace
rustup target add wasm32-unknown-unknown
cargo install trunk
trunk serve web/index.html
```

生产：

```bash
trunk build web/index.html --release --dist web/dist
cargo build --workspace --release
deploy/screen-start.sh config/paper.toml
```

daemon 启动时始终在 `rpc.http_listen` 启动 jsonrpsee HTTP/WebSocket server；若
`[web].enabled = true`，再在 `web.listen` 提供静态页面：

1. 校验 RPC 与 Web 不能使用同一监听地址；
2. 注册共享 RPC module 并应用请求大小、并发数和 Origin 限制；
3. 校验 `web/dist/index.html` 并提供静态文件；
4. HTTP 关键任务异常退出时触发 supervisor，使 daemon 非零退出并由现有 screen runner
   拉起。

静态资源不嵌进二进制，便于单独重建 Web；发布包必须把 `web/dist` 与 release binary
一起交付。资源文件名带内容 hash，`index.html` 使用 no-cache，hash asset 使用长期
immutable cache。

## 9. 测试

### `rpc-types`

- 每个 params/result 的 JSON fixture round-trip；
- `QuantRpcClient`/`QuantRpcServer` codegen 和 method name fixture；
- UUID、UTC 时间和 enum 的稳定 JSON 表示；
- object/positional params 兼容性决策和 RPC version fixture；
- `cargo check -p quant-rpc-types --target wasm32-unknown-unknown`。

### 后端

- CLI `HttpClient` 和 Web `WasmClient` contract 一致；
- body、超时、并发和 origin 限制；
- token 缺失/错误；
- mutation 在 client disconnect 后仍完成审计；
- capability 与已注册 method 一致；
- `HttpClient` CLI 回归测试。

### Web

- page model/reducer 单元测试；
- `WasmClient` 对成功、业务错误、协议错误和超时的测试；
- 表单验证和危险操作确认；
- polling 不重叠、页面隐藏降频；
- `wasm-bindgen-test` 覆盖关键组件；
- Playwright 或等价浏览器测试覆盖：
  - 登录 token；
  - Dashboard 加载；
  - 策略启动/暂停；
  - order preview；
  - paper submit 确认；
  - unknown order 页面；
  - emergency stop。

CI 最低命令：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p quant-web --target wasm32-unknown-unknown
trunk build web/index.html --release
```

## 10. 原始实施阶段（已完成的历史计划）

### 阶段 A：协议抽取

1. 创建 workspace 和 `rpc-types`；
2. 使用 `#[rpc(client, server)]` 定义 API，抽取公共 DTO；
3. 通过 fixture 决定保持 RPC v1 参数形状或显式升级 v2；
4. 后端业务行为和已有业务测试保持不变。

验收：CLI 全部可用，RPC JSON fixture 无不兼容变化，workspace 测试通过。

### 阶段 B：jsonrpsee 服务端与 CLI

1. 实现生成的 `QuantRpcServer` 并注册 `RpcModule`；
2. 增加 HTTP/WebSocket listener、Origin 限制和 supervisor；
3. CLI 改用生成的 `QuantRpcClient` 和 `HttpClient`；
4. 删除 UDS server/client、socket 配置和运行时文件处理。

验收：CLI/Web API 共享 RPC 类型和方法清单，安全默认使用 loopback。

### 阶段 C：Yew 外壳

1. 完成 `yew-bootstrap` compatibility spike 并锁定 crate/CSS 版本；
2. 创建 Trunk 项目、路由、布局、主题和 `WasmClient`；
3. 实现版本协商、认证页和全局连接 banner。

验收：WASM release 构建可由 daemon 同源打开。

### 阶段 D：只读功能

实现 Dashboard、Portfolio、Market Data、Strategies、Performance、Orders 和
Operations 的只读视图。

验收：CLI 可见的主要状态都可在 Web 查看，错误/空/loading 状态明确。

### 阶段 E：受控操作

依次实现低风险操作、策略管理、数据任务、订单 preview、paper submit/cancel、
execution enable/disable 和 safety 操作。

验收：所有 mutation 经过共享 typed RPC；危险操作有二次确认；unknown outcome
不会自动重试。

### 阶段 F：长期运行验收

1. screen 中运行 daemon、jsonrpsee HTTP RPC 和 Web 静态服务；
2. Gateway 断线重连；
3. 浏览器刷新/关闭时 mutation 结果可恢复；
4. 连续 paper soak test；
5. 备份恢复与 TCP/HTTP CLI 回归。

## 11. 完成定义

以下条件全部满足才算 Web 首版完成：

- 三个 package 位于同一 workspace；
- RPC 公共契约只定义在 `rpc-types`；
- daemon、CLI 和 Web 使用同一 `jsonrpsee` API trait；
- Web 不直接访问数据库或 IBKR；
- CLI 使用 `HttpClient`，Web 使用 `WasmClient`，代码中不存在 UDS transport；
- HTTP 默认监听 loopback 并严格校验 Origin；外部监听由部署层补充认证和 TLS；
- `yew-bootstrap`、Yew 与 Bootstrap CSS 版本被精确锁定；
- Dashboard、策略、绩效、订单、监控和对账可用；
- paper 危险操作有确认、幂等和 unknown outcome 处理；
- workspace、WASM、Trunk 和浏览器关键流程测试通过；
- 现有 screen 后台工作流不被破坏。

## 12. 明确不做

首版不包含：

- 公网直接暴露 daemon；
- 多用户、角色和权限系统；
- 浏览器中保存 IBKR 密码；
- Web 编辑完整 TOML；
- 绕过 daemon 的数据库查询；
- 自动 live approval；
- 浏览器断线后自动重提订单；
- WebSocket/SSE 推送；
- 原生移动端。

这些限制确保 Web 只是现有安全交易平台的友好控制面，而不是第二套未经风控的交易
系统。
