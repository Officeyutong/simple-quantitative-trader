# Simple Quantitative Trader

个人使用的 IBKR 量化交易后台，使用 Rust、Tokio、`ibapi`、DuckDB 和
Parquet。

当前版本提供：

- 长期运行的 daemon 和同一二进制 CLI；
- 由 `jsonrpsee` 提供、监听地址可配置的 HTTP/WebSocket JSON-RPC 2.0；
- Yew + `yew-bootstrap` 本地 Web 控制台；
- IB Gateway/TWS 连接、断线检测和指数退避重连；
- managed accounts 校验；
- IBKR 合约搜索；
- 内部 InstrumentId 主数据、当前持仓和账户/PnL 历史；
- 历史行情下载、质量检查和 Parquet 原子落盘；
- Parquet 文件 DuckDB manifest；
- Parquet 校验和、数据快照和确定性回测；
- 六种内置策略、持久化运行状态、1 分钟/5 秒实时 Bar 和可复用回测核心；
- 仅限 paper 的目标仓位自动执行，支持单腿、多腿与目标空头仓位；
- 市价单/限价单风险预览；
- `trading_enabled`、最大数量和最大名义价值检查；
- 幂等订单意图、风险决策和提交记录；
- 订单提交、撤单、状态诊断和 broker 事件审计；
- 结构化日志、进程锁、数据库迁移和优雅停止。
- 健康检查、一致性联合备份、`screen` 守护脚本和持久化紧急停止。

完整架构见 [design.md](design.md)。

Web/RPC workspace 设计见 [web-design.md](web-design.md)。

## 最简运行工作流

首次运行先准备配置，并在其中确认 paper 账户、IBKR 端口、风险限额以及
`connect_on_start = true`：

```bash
cp config/example.toml config/paper.toml
cargo build --release
```

随后每次运行只需：

```bash
# 1. 先启动并登录 IB Gateway paper 账户

# 2. 在 screen 中启动 daemon
deploy/screen-start.sh config/paper.toml

# 3. 确认 daemon、IBKR 和对账状态
BIN=target/release/simple-quantitative-trader
CFG=config/paper.toml
$BIN --config "$CFG" status
$BIN --config "$CFG" ibkr status
$BIN --config "$CFG" reconcile status

# 4. 检查行情、策略和执行配置
$BIN --config "$CFG" market-data subscriptions
$BIN --config "$CFG" strategy list
$BIN --config "$CFG" strategy execution list

# 5. 检查运行结果
$BIN --config "$CFG" monitor alerts
$BIN --config "$CFG" positions
$BIN --config "$CFG" order list
$BIN --config "$CFG" executions
```

浏览器控制台：

```text
http://127.0.0.1:8080
```

CLI 通过 HTTP 调用 RPC，Web 前端通过同一 `jsonrpsee` server 的 WebSocket
transport 调用 RPC。`config/example.toml` 默认监听 `127.0.0.1:8787` 和
`127.0.0.1:8080`；实际边界由 `rpc.http_listen` 与 `web.listen` 决定。
仓库中的长期 paper 配置可能面向局域网监听，必须只在可信网络或带 TLS、认证和
访问控制的反向代理后使用，绝不能直接暴露到公网。

如果 `ibkr status` 没有进入 Ready，可手动请求连接：

```bash
$BIN --config "$CFG" ibkr connect
```

新策略需要先创建、启动、配置执行层，并在核对账户和目标仓位后显式启用：

```bash
$BIN --config "$CFG" strategy start <STRATEGY_ID>
$BIN --config "$CFG" strategy execution enable <STRATEGY_ID> --confirm
```

不要对尚未检查的策略直接启用执行。运行期间 daemon 会自动计算信号、执行已授权的
paper 策略、记录成交并生成绩效快照。查看策略收益：

```bash
$BIN --config "$CFG" performance report <STRATEGY_ID> \
  --initial-capital 100000 --benchmark-conid 756733
```

离开后台会话无需停止程序；执行 `screen -r quant-trader` 后按 `Ctrl-A`、`D`。
需要真正停止时：

```bash
deploy/screen-stop.sh config/paper.toml
```

更完整的策略配置、日常检查和盈利评估标准见
[长期 Paper 运行与盈利能力评估](#长期-paper-运行与盈利能力评估)。

## 启动

复制并修改示例配置，尤其是 IBKR 端口、client ID 和可选账户白名单：

```bash
cp config/example.toml config/local.toml
cargo run -- --config config/local.toml daemon
```

启动前必须检查配置中的 `risk.trading_enabled`；示例配置可能为了 paper 验证而
开启它。即使该值为 `true`，策略执行仍默认关闭，必须逐个策略显式配置并确认启用。
live 自动执行始终被程序硬性禁止。

## Web 控制台

项目是包含后端、`quant-rpc-types` 和 `quant-web` 的 Cargo workspace。构建后端和
Web：

首次安装、局域网访问、后台运行和故障排查的完整步骤见 [setup.md](setup.md)。

```bash
cargo build --workspace --release
cd web
env -u NO_COLOR trunk build --release
cd ..
```

配置：

```toml
[rpc]
http_listen = "127.0.0.1:8787"
allowed_web_origin = "http://127.0.0.1:8080"

[web]
enabled = true
listen = "127.0.0.1:8080"
static_dir = "../web/dist"
```

daemon 启动后打开 `http://127.0.0.1:8080`。控制台当前提供总览、证券搜索、策略、
策略状态、策略绩效、回测、下载任务、交易成本、交易日历、均线策略向导、均值回归
向导、Paper 验证、订单与成交、运行维护、实时日志、RPC 工具和 RPC 设置。
持仓、策略执行配置、执行动作、订单、成交、告警、监控指标和系统状态均以表格展示，
页面每 5 秒自动刷新，也可以手动立即刷新。所有操作都调用 daemon RPC，不直接访问
DuckDB 或 IB Gateway。

“下载任务”页面通过服务端分页展示数据库中的全部历史数据任务，包括真实运行状态、
IBKR worker 状态、队列位置、证券与交易时段、完整请求范围、游标进度、已完成分片、
重试次数和最近错误。活动任务可在该页面取消；`data.jobs` 接受 `page` 与 `page_size`
并返回 `total_items`、`total_pages` 和全局队列摘要，因此翻页不会丢失当前 worker 信息。

“策略绩效”页面可选择策略和初始资金，展示净收益、收益率、最大回撤、胜率、
Sharpe、Sortino、交易统计及历史绩效快照。绩效只归因于该策略执行层产生并实际成交
的订单；时间均按浏览器本地时区显示。

“Paper 验证”页面提供合约搜索与选择、行情订阅、行情健康检查、验证策略创建、
执行配置、策略启动和执行启用的分步向导。“RPC 工具”页面从共享
`quant-rpc-types::ALL_METHODS` 读取方法列表，覆盖 CLI 使用的全部 RPC；查询可直接
执行，变更操作需要额外勾选确认，并提供常见参数模板。常用操作仍应优先使用专用
页面，RPC 工具用于尚未制作专用表单的高级功能和诊断。

在 Web 控制台的“RPC 设置”中可以修改 WebSocket RPC 地址，例如
`ws://127.0.0.1:8787` 或由反向代理提供的 `wss://quant.example.com`。地址会保存在
当前浏览器的 LocalStorage 中，刷新或重新打开页面后仍然生效。保存新地址后页面会
立即重新连接，所有查询和变更操作都会使用该地址。

如果 Web 页面和 daemon 不在同一台机器上，不要直接把无 TLS 的交易 RPC 暴露到
公网。应使用 SSH 隧道，或使用带 TLS 和身份认证的 WebSocket 反向代理访问 daemon
的 loopback listener，并把 daemon 的 `rpc.allowed_web_origin` 精确设置为 Web
页面的 Origin，例如 `https://quant.example.com`。

如果部署环境无法预先确定 Web Origin，也可以显式配置通配符：

```toml
[rpc]
allowed_web_origin = "*"
```

这会允许任意网站发起浏览器 RPC 请求，daemon 启动时会记录安全警告。它不改变
`rpc.http_listen` 的值；如果该监听地址不是 loopback，任意 Origin 与外部监听组合
会显著扩大攻击面。必须由防火墙或反向代理提供可靠的 TLS、身份认证和访问控制。
能使用精确 Origin 时仍应优先使用精确值。暂不支持
`https://*.example.com` 这类部分通配模式。

另一个终端中：

```bash
cargo run -- --config config/local.toml status
cargo run -- --config config/local.toml ibkr connect
cargo run -- --config config/local.toml account summary
cargo run -- --config config/local.toml account pnl
cargo run -- --config config/local.toml instrument search AAPL
cargo run -- --config config/local.toml instrument list
cargo run -- --config config/local.toml positions
```

账户摘要、持仓和账户级 PnL 在 IBKR 连接 Ready 后由后台持续订阅，CLI 查询的是
DuckDB 中最新的本地快照。断线时订阅会取消，重连后自动重建。

## 实时行情

行情订阅会持久化，并在 daemon 或 IBKR 重连后自动恢复。没有实时行情权限时，
程序请求 IBKR delayed fallback：

```bash
cargo run -- --config config/local.toml market-data subscribe \
  --conid 756733 \
  --symbol SPY \
  --exchange SMART \
  --primary-exchange ARCA \
  --local-symbol SPY

cargo run -- --config config/local.toml market-data subscriptions
cargo run -- --config config/local.toml market-data quote --conid 756733
cargo run -- --config config/local.toml market-data health --conid 756733
cargo run -- --config config/local.toml market-data bars --conid 756733 --limit 100
cargo run -- --config config/local.toml market-data unsubscribe --conid 756733
```

策略在未配置并启用执行层时仅生成可审计信号，不会自动下单：

```bash
cargo run -- --config config/local.toml strategy create-ma \
  --name spy-ma --conid 756733 --short-window 5 --long-window 20
cargo run -- --config config/local.toml strategy list
cargo run -- --config config/local.toml strategy start <STRATEGY_ID>
cargo run -- --config config/local.toml strategy signals <STRATEGY_ID> --limit 100
cargo run -- --config config/local.toml strategy pause <STRATEGY_ID>
```

策略核心按 `model`、`engine`、`web` 三类 crate 拆分；实时运行器和回测通过
`strategy-api` 的 `Strategy` trait 共用同一算法，前后端 Catalog 负责注册。
完整扩展流程、`close_threshold` 和布林带 + RSI 均值回归示例见
[STRATEGIES.md](STRATEGIES.md)。通用创建接口：

```bash
cargo run -- --config config/local.toml strategy kinds
cargo run -- --config config/local.toml strategy create \
  --name spy-threshold \
  --kind close_threshold \
  --config-json '{"conid":756733,"buy_below":600,"sell_above":800}'

cargo run -- --config config/local.toml strategy create \
  --name spy-bollinger-rsi \
  --kind bollinger_rsi_mean_reversion \
  --config-json '{"conid":756733,"bar_timeframe":"1m"}'
```

可选的策略执行层默认关闭且仅支持 paper。它采用目标仓位语义，并复用完整的
`order.submit` 风控链路：

```bash
cargo run -- --config config/local.toml strategy execution configure \
  --strategy-id <STRATEGY_ID> --account DU123456 \
  --target-quantity 1 --conid 756733 --symbol SPY \
  --primary-exchange ARCA --local-symbol SPY
cargo run -- --config config/local.toml strategy execution enable \
  <STRATEGY_ID> --confirm
cargo run -- --config config/local.toml strategy execution actions --limit 100
cargo run -- --config config/local.toml strategy execution disable <STRATEGY_ID>
```

启用还要求 paper 环境和 `risk.trading_enabled = true`。live 自动执行被硬性禁止。

本地 Parquet 回测采用“收盘信号、下一根 Bar 开盘成交”，防止未来函数：

```bash
cargo run -- --config config/local.toml backtest run \
  --conid 265598 --timeframe 1d \
  --start 2026-07-20T00:00:00Z --end 2026-07-25T00:00:00Z \
  --short-window 1 --long-window 2 --quantity 1 \
  --cost-model-id <COST_MODEL_ID> --seed 42
cargo run -- --config config/local.toml backtest list
```

Web 回测选择已有策略后，会从该策略的持久化配置自动锁定证券 `conid` 和 Bar 周期，
不再要求重复搜索证券，也不能用另一只证券或其他周期替换。后端同样以
`strategy_id` 对应的保存配置为权威，忽略请求中冲突的 kind、config、conid 和
timeframe。需要测试另一只证券时，应创建新的策略配置。Parquet 历史回测不允许用
其他周期冒充策略绑定周期；`5s` 策略会下载真实 5 秒历史 Bar，并使用这些数据回测。
回测开始前，前端和后端都会要求所选范围已经被匹配的历史下载任务完整抓取；仅存在
少量与范围重叠的 Parquet 文件不会被视为完整数据，也不能启动部分范围回测。
Web 回测不再单独填写佣金和滑点：已保存策略必须先在“交易成本”页面绑定数据库费用
模型，后端会强制使用该绑定并校验模型币种与证券币种。未绑定策略的 CLI/RPC 临时
回测必须显式提供数据库 `cost_model_id`。每次回测会把完整模型快照保存到运行参数，
之后修改模型不会改写历史结果。

`backtest.get` 不会把长周期 5 秒回测的全部权益点一次性写入 JSON。它默认返回最多
2,000 个均匀抽样点并保留首尾，`equity_sampling` 会说明完整点数、返回点数和步长；
收益与回撤指标仍由完整权益序列计算。成交记录通过 `trade_page`、`trade_page_size`
分页，单页上限 500 条，从而避免详情响应超过 RPC 的 10 MiB 限制。

`quote` 会同时返回最新 ticks 和订阅状态。IBKR 拒绝订阅时，错误会持久化并显示，
后台每 15 秒受控重试。

`risk.max_market_data_age_seconds` 控制报价最大允许年龄，默认 30 秒。新开仓要求
行情状态为 `fresh`；缺失、失败或过期行情都会阻止提交。基于当前会话持仓的
严格平仓不受行情故障阻止。成交价 tick 会聚合成 DuckDB 中的一分钟 OHLC，
并同时聚合 5 秒 OHLC；下一个对应区间的首个成交到达时，前一根 Bar 标记为
final。没有成交的区间不会合成空 Bar。

## 历史行情

先从 `instrument search` 的候选结果确认 conid 和合约字段，然后执行：

```bash
cargo run -- --config config/local.toml data backfill \
  --conid 265598 \
  --symbol AAPL \
  --timeframe 1d \
  --start 2026-07-20T00:00:00Z \
  --end 2026-07-27T00:00:00Z
```

支持 `5s`、`1m`、`5m`、`15m`、`30m`、`1h` 和 `1d`。其中 `5s` 按每小时分片
请求 IBKR，较长范围会产生较多请求。结果写入
`data/lake/bars/timeframe=.../conid=...`，时间统一为 UTC。
backfill 会返回持久化 Job ID，由 daemon 在 IBKR Ready 后后台执行：

同一 `conid`、Bar 周期和 `outside_rth` 下，重叠的未完成请求不会重复入队；系统会复用
最早任务并扩展其范围，daemon 启动时也会折叠旧版本遗留的重叠任务。`data jobs` 同时
返回持久化 `state`、面向操作员的 `runtime_state`、`queue_position` 和 `jobs_ahead`：
队首在 IBKR Ready 时显示为正在下载，其余任务明确显示为排队中。Web“下载任务”页面
使用 `data.jobs` 的服务端分页查看全部历史任务，而不是只截取最近一批记录。

```bash
cargo run -- --config config/local.toml data jobs
cargo run -- --config config/local.toml data cancel <JOB_ID>
cargo run -- --config config/local.toml data coverage \
  --conid 756733 --timeframe 1d \
  --start 2026-07-20T00:00:00Z --end 2026-07-22T00:00:00Z
```

数据完整性和可复现快照：

```bash
cargo run -- --config config/local.toml data verify
cargo run -- --config config/local.toml data snapshot create \
  --name bars-before-research --dataset bars
cargo run -- --config config/local.toml data snapshot list
```

Job 会按 timeframe 自动切片、串行 pacing、失败重试 3 次，并在 daemon 重启后
从保存的 cursor 继续。cursor 只会在 IBKR 请求成功且数据已经落盘后推进，因此
coverage 会合并同一 `conid`、timeframe 和 `outside_rth` 的成功抓取区间，报告
`backtest_ready`、已验证范围和未验证范围。只有成功区间覆盖完整请求且存在相应
Parquet Bar 时才能回测。`raw_gaps` 仍作为自然时间诊断保留；夜间、周末和休市时段
即使没有 Bar，只要对应分片已成功请求，也不会造成错误的“不完整”判断。
请求端点落入夏令时结束时的重复本地小时会自动扩大仅用于 IBKR 抓取的窗口，再过滤
回原始 UTC 范围，避免第三方解码器因歧义时间退出；Bar 自身的 epoch 时间不会改变。

## 运维与发布安全

推荐的后台运行方式是 `screen`（不需要 systemd）。仓库自带异常退出自动拉起、
日志记录、优雅停止和状态脚本：

```bash
cargo build --release
cp config/example.toml config/paper.toml
# 将 paper.toml 中 connect_on_start 设为 true，并复核账户和风险限制
deploy/screen-start.sh config/paper.toml
deploy/screen-status.sh config/paper.toml
screen -r quant-trader
# Ctrl-A D 离开会话
deploy/screen-stop.sh config/paper.toml
```

daemon 内置任务监督：任何关键后台任务（broker 事件持久化、策略评估、策略执行、
历史任务、自动对账）panic 或意外退出时，daemon 会记录错误、触发优雅停机并以
非零退出码结束，而不是带病继续运行。`screen-run.sh` 会记录退出码、等待 5 秒后
重新启动；`screen-stop.sh` 通过停止标志阻止再次拉起。日志保存在 `logs/`。
Storage 互斥锁被 panic 污染时会自动恢复，不会级联崩溃。

```bash
cargo run -- --config config/local.toml health
cargo run -- --config config/local.toml backup create
cargo run -- --config config/local.toml backup list

cargo run -- --config config/local.toml safety status
cargo run -- --config config/local.toml safety set \
  --mode emergency_stop --note "operator drill" --confirm
cargo run -- --config config/local.toml safety set \
  --mode normal --note "drill reset" --confirm
```

紧急停止和策略暂停保存在 DuckDB 中，daemon 重启不会自动清除。live 环境除了
`trading_enabled`，还必须有显式 live approval 且 conid 位于白名单。默认
`live_approved = false`。

数据库 schema 版本高于当前程序支持的版本时 daemon 拒绝启动，防止旧二进制静默
运行新数据。`QUANT__RISK__TRADING_ENABLED` 等环境变量覆盖会在启动日志中显式
警告。

## 长期绩效、监控、FX 与交易日历

```bash
quant performance report <STRATEGY_ID> \
  --initial-capital 100000 --benchmark-conid 756733
quant performance snapshots <STRATEGY_ID> --limit 100

quant monitor metrics
quant monitor alerts
quant monitor acknowledge <ALERT_ID> --note "reviewed"
```

绩效按 strategy action 归因，报告毛/净 PnL、佣金、换手率、胜率、最大回撤、
Sharpe、Sortino、每日权益和可选基准超额收益。启用策略会按 `[monitoring]`
周期保存快照。告警持久化 IBKR/对账异常、失败或延迟行情、Unknown 订单、失败
策略 action 和绩效快照错误。

佣金来自 IBKR 的实际 `CommissionReport`，不是程序按固定费率推算；非基础币种佣金
在生成报告时使用当前新鲜 FX 汇率换算，因此不同时间生成的历史快照可能随汇率轻微
变化。累计佣金会随成交增加。“交易成本”页面可在 DuckDB 中维护每笔固定费、每股
费用、成交额比例费、最低收费、卖出税费、点差和滑点模型，并为策略启用下单前成本
门控。模型不写入 TOML。
短周期和高换手策略仍应单独评估费用。

成本门控将信号指标差换算成 bps，并与预计完整往返成本乘安全倍数后的门槛比较。
固定费用会使小额订单的门槛自动升高，比例费用则随成交额变化。系统还会使用该策略
历史实际 `CommissionReport` 的单边有效费率 P90（如果存在）与配置估算取更保守者。
不满足条件的 action 记为 `skipped`，并保存名义金额、预计往返成本、信号强度和所需
强度。达到配置的最少平仓交易数后，若佣金/毛利润超过上限，执行配置会自动停用。
策略执行配置关闭期间产生的买卖信号也会保存为 `skipped` action，并标记
`execution_disabled`，避免出现“有信号但没有动作记录”的审计空白；这类历史信号在
重新启用后不会补单。

非账户基础币种必须有新鲜 FX 汇率。daemon 每隔
`risk.fx_rate_refresh_seconds` 秒从 IBKR Account Updates 获取 `ExchangeRate`，
并写入 DuckDB 的 `fx_rates`；该刷新周期必须小于
`risk.max_fx_rate_age_seconds`。IBKR 账户基础币种必须与 `risk.base_currency` 一致，
否则 `ExchangeRate` 的报价方向无法满足风控与绩效换算。也可人工维护汇率：

```bash
quant fx set --base USD --quote HKD --rate 7.84 --source manual
quant fx list
```

自动执行通过 IBKR `ContractDetails` 获取并缓存证券的交易日历。正常订单使用
`liquidHours`，开启盘前盘后时使用 `tradingHours`；IBKR 返回的交易所本地时间会按
`timeZoneId`（包括夏令时）转换为 UTC。日历缓存每 6 小时按需刷新，支持同一天多个
交易区间。日历缺失、刷新失败或当前不在对应交易区间时均拒绝自动下单，不再因
`outside_rth=true` 绕过交易时段检查。

人工 session 仍可用于诊断或 IBKR 不支持的合约：

```bash
quant calendar add --exchange SEHK --date 2026-07-28 \
  --opens-at 2026-07-28T01:30:00Z --closes-at 2026-07-28T08:00:00Z
quant calendar status --exchange SEHK
quant calendar status --exchange SEHK --outside-rth
quant calendar list --exchange SEHK
```

Web 的“交易日历”页面可从策略执行配置选择证券，立即调用 IBKR 刷新日历，分别查看
正常/扩展时段当前状态和缓存区间，也可按浏览器本地时间人工补录正常时段。

## 长期 Paper 运行与盈利能力评估

长期运行应使用独立配置，不要直接修改示例文件：

```bash
cp config/example.toml config/paper.toml
chmod 600 config/paper.toml
```

在 `config/paper.toml` 中确认：

```toml
[app]
environment = "paper"

[ibkr]
connect_on_start = true
host = "127.0.0.1"
port = 4002
client_id = 17

[risk]
trading_enabled = true
base_currency = "HKD"
fx_rate_refresh_seconds = 300
max_fx_rate_age_seconds = 3600
max_order_quantity = 100.0
max_position_quantity = 500.0
max_order_notional = 50000.0
max_gross_exposure = 200000.0
max_net_exposure = 200000.0
max_open_orders = 10
max_orders_per_minute = 5
max_daily_loss = 5000.0

[monitoring]
enabled = true
interval_seconds = 30
performance_snapshot_seconds = 300
performance_initial_capital = 100000.0
alert_on_delayed_market_data = true
```

IB Gateway 需要保持登录 paper 账户、开放 API、关闭 Read-Only，并使监听端口与配置
一致。Gateway 自身的每日重新登录或自动重启需要在 Gateway 中单独配置。

### 创建并启用策略

均线策略向导支持三种彼此独立的实时策略：

- `moving_average_cross`：使用已完成的 1 分钟 Bar；
- `moving_average_cross_5s`：使用已完成的 5 秒 Bar；
- `moving_average_cross_v2`：可选 1 分钟或 5 秒、SMA/EMA，并提供确认、冷却、
  ATR 与趋势过滤。

三者可以同时存在。daemon 会从实时成交 Tick 同时聚合 1 分钟和 5 秒 OHLC，
策略只在对应周期的 Bar 完成后计算，且同一 Bar 只计算一次。5 秒策略需要持续的
实时成交 Tick；没有成交的 5 秒区间不会凭空生成 Bar。

查看策略类型并创建策略：

```bash
BIN=target/release/simple-quantitative-trader
CFG=config/paper.toml

$BIN --config "$CFG" strategy kinds
$BIN --config "$CFG" strategy create-ma \
  --name spy-ma-5-20 --conid 756733 \
  --short-window 5 --long-window 20
$BIN --config "$CFG" strategy start <STRATEGY_ID>
```

订阅行情、设置目标仓位并显式开启自动执行：

```bash
$BIN --config "$CFG" market-data subscribe \
  --conid 756733 --symbol SPY --exchange SMART \
  --primary-exchange ARCA --local-symbol SPY

$BIN --config "$CFG" strategy execution configure \
  --strategy-id <STRATEGY_ID> --account <PAPER_ACCOUNT> \
  --target-quantity 100 --conid 756733 --symbol SPY \
  --primary-exchange ARCA --local-symbol SPY

$BIN --config "$CFG" strategy execution enable \
  <STRATEGY_ID> --confirm
```

建议先使用小仓位确认信号、订单、成交、持仓和绩效归因一致，再增加模拟仓位。放大
paper 仓位只会线性放大损益，不能提高盈利结论的可信度。自动执行只接受实时
bid/ask；delayed 行情可以用于观察，但会被执行层拒绝。

用于网页全链路验证时，可创建确定性交替信号策略：

```bash
$BIN --config "$CFG" strategy create \
  --name paper-web-round-trip \
  --kind paper_round_trip \
  --config-json '{"conid":<SEARCHED_CONID>,"phase_bars":1}'
```

该策略每个 phase 在 `buy`/`sell` 间切换，专用于 paper 验证，不代表具有盈利能力。
应先用 `instrument search` 取得当前开市股票或 ETF 的真实 conid 和合约字段，并
确认 `conid > 0`、`security_type = STK` 以及实时 Bid/Ask。当前执行层不支持 CASH
外汇；完整配置步骤见 [STRATEGIES.md](STRATEGIES.md)。

### 使用 screen 挂在后台

```bash
cargo build --release
deploy/screen-start.sh config/paper.toml
deploy/screen-status.sh config/paper.toml
screen -r quant-trader
```

在 screen 中按 `Ctrl-A`，再按 `D`，可以离开会话而不停止程序。日志位于 `logs/`：

```bash
ls -lt logs/
tail -f logs/quant-*.log
```

停止时使用优雅停止脚本，不要直接 kill：

```bash
deploy/screen-stop.sh config/paper.toml
```

### 日常检查

建议每天至少执行一次：

```bash
$BIN --config "$CFG" status
$BIN --config "$CFG" health
$BIN --config "$CFG" ibkr status
$BIN --config "$CFG" reconcile status
$BIN --config "$CFG" market-data health --conid 756733
$BIN --config "$CFG" monitor metrics
$BIN --config "$CFG" monitor alerts
$BIN --config "$CFG" positions
$BIN --config "$CFG" order list
$BIN --config "$CFG" executions
```

重点检查 IBKR 是否 Ready、对账是否健康、行情是否实时且新鲜，以及是否存在
`unknown` 订单、失败 action 或无法解释的持仓差异。人工核对告警后可以确认：

```bash
$BIN --config "$CFG" monitor acknowledge <ALERT_ID> \
  --note "已核对原因"
```

acknowledge 只表示已经审阅，不会自动修复告警原因。

### 评估策略

```bash
$BIN --config "$CFG" performance report <STRATEGY_ID> \
  --initial-capital 100000 --benchmark-conid 756733
$BIN --config "$CFG" performance snapshots <STRATEGY_ID> --limit 500
```

应主要观察净 PnL、最大回撤、Sharpe、Sortino、胜率、换手率、交易次数和相对基准
收益，而不是绝对盈利金额或单笔大额盈利。建议使用以下最低观察标准：

- 固定策略和参数连续运行至少 4–8 周；
- 覆盖不同的市场状态；
- 扣除佣金后仍然盈利；
- 最大回撤没有超过预设风险预算；
- 相对买入持有基准有可解释的改善；
- Gateway、网络或 daemon 重启后没有重复订单；
- 没有未解决的 `unknown` 订单和持仓差异。

均线交叉的 `min_gap` 只过滤均线差距，不是预期收益保证。尤其是 5 秒
策略，在存在按成交额收费、最低收费、印花税或平台费的市场，频繁反转很可能让费用
超过毛收益。应在“交易成本”页面维护并绑定按市场区分的数据库费用模型，并以实际
paper `CommissionReport` 校准。实时成本门控和回测现在共享同一套固定费、每股费、
比例费、最低费、卖出税费、点差和滑点公式；成本门控是否启用不影响回测扣费。

正式评估开始后应冻结参数。需要调参时创建新的策略 ID，将其视为新的实验，避免
把样本内调参与样本外结果混在一起。每天或修改策略前创建一致性备份：

```bash
$BIN --config "$CFG" backup create
$BIN --config "$CFG" backup list
```

paper forward test 只能提供策略稳定性和模拟盈利证据，不等于实盘盈利保证。只有在
数周运行、故障恢复演练和人工审核全部通过后，才应考虑最小额度的实盘准入。

## 订单安全

先运行风险预览：

```bash
cargo run -- --config config/local.toml order preview \
  --idempotency-key preview-001 \
  --account DU123456 \
  --conid 265598 \
  --symbol AAPL \
  --side buy \
  --quantity 1 \
  --order-type limit \
  --limit-price 150
```

实际提交还要求配置中开启交易，并提供显式确认：

```bash
cargo run -- --config config/local.toml order submit \
  --idempotency-key paper-order-001 \
  --account DU123456 \
  --conid 265598 \
  --symbol AAPL \
  --side buy \
  --quantity 1 \
  --order-type limit \
  --limit-price 150 \
  --confirm
```

不要重复使用不同 idempotency key 重试状态不明的订单。

订单提交结果分三类：

- 成功：返回内部 order ID 和 broker order ID；
- 明确拒绝：intent 标记为 `risk_rejected`/`broker_rejected`/`blocked`；
- **结果未知**：等待 IBKR 确认超时、响应流中断，或 broker 已受单但本地记录
  失败（连接会话丢失、存储写入失败）时，intent 标记为 `unknown` 并返回错误码
  `-32026`。此时订单可能仍在 IBKR 存活，必须先 `reconcile` 查看开放订单，
  绝不能换 key 重发。daemon 在风险批准后、broker 确认前崩溃时，重启会把滞留
  的 `approved` intent 同样标记为 `unknown`。

`unknown` intent 会持续占用风控额度（计入活跃订单数和同合约持仓投影），并阻塞
同一合约的自动执行。操作员在通过 `reconcile`、开放订单和成交记录确认真实结果
后，可用 `order.intent.resolve`（参数：`order_intent_id`、`note`、
`confirm=true`）人工解除；解除动作会写入审计记录。

订单列表会保存 `remaining_quantity`、`last_fill_price`、`why_held` 和
`market_cap_price`。IBKR 的 open/completed order 回调还会写入
`broker_order_events`，保留状态、拒绝原因和警告文本。诊断“为什么没成交”时，应
同时检查订单当前状态、剩余数量、`why_held`、限价与实时 bid/ask、交易时段以及事件
历史；`IBKR 无活动订单`只表示当前 broker open-orders 集合中不存在它，不等于已经
成交。

所有 submit 尝试（包括被紧急停止、live 未批准、账户校验或就绪门控拒绝的）
都会持久化 order intent 和 risk decision 审计记录，并消耗对应 idempotency
key。风控检查与 intent 写入在同一个临界区内完成；提交中（`approved`）和结果
不明（`unknown`）的 intent 也计入活跃订单数与同合约持仓投影，因此并发提交
无法一起挤过持仓、敞口、活跃订单数和速率限制。市价单的单笔名义额检查优先采用
本地最新行情价，仅在本地无行情时才使用自报的 `--estimated-price`。持仓中存在
无法用新鲜 FX 汇率折算为基础币种的币种时，开仓会以 `FX_RATE_UNAVAILABLE`
拒绝，而不是低估敞口；严格平仓不受此限制（无法折算时按名义 1:1 参与单笔
检查）。

组合风险限制在 `[risk]` 中配置：

```toml
max_position_quantity = 1000.0
max_gross_exposure = 100000.0
max_net_exposure = 100000.0
max_open_orders = 20
max_orders_per_minute = 10
max_daily_loss = 1000.0
max_price_deviation_bps = 500.0
max_account_data_age_seconds = 120
```

`order preview` 会返回投影持仓、gross/net exposure、活跃订单数、最近一分钟
订单数、账户 PnL 和价格偏离。账户或持仓数据过期时禁止开仓；严格平仓可以绕过
持仓、敞口和亏损上限，但仍受下单速率限制。当前 IBKR 持仓快照成功完成后，即使
账户完全空仓、没有任何 `positions_current` 行，也会按“已确认的零持仓”参与风控；
尚在同步或超过 `max_account_data_age_seconds` 的快照仍会阻止开仓。

daemon 每次连接 IBKR 后会自动执行订单对账，此后每 10 分钟周期性重复，使会话
中期出现的未知订单和外部订单及时进入就绪门控。对账快照读取超时会直接失败并
重试，不会以截断的快照误判本地订单缺失。对账完成前会拒绝提交；存在
blocking difference 时进入 `Degraded`，只允许基于当前会话新鲜持仓、方向朝向
零且不穿过零点的纯平仓订单。撤单始终可用。可以检查状态和差异：

```bash
cargo run -- --config config/local.toml reconcile status
cargo run -- --config config/local.toml reconcile differences
cargo run -- --config config/local.toml reconcile acknowledge \
  --difference-id <UUID> \
  --note "人工核对说明"
cargo run -- --config config/local.toml reconcile
```

确认差异只保存审计记录，不会解除 `Degraded`。需要先处理 IBKR 侧开放订单或
本地状态问题，再重新对账；只有新的对账结果无 blocking difference 才会恢复
`healthy`。

## 测试

```bash
cargo check
cargo test
```
