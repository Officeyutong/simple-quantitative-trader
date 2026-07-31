# 策略说明

本文档按当前源码整理项目中所有已注册策略。策略实现和注册表位于
`src/strategy.rs`；实时运行器与回测引擎通过同一个 `Strategy` trait 调用同一份
信号逻辑。

## 1. 策略总览

| `kind` | 用途 | Bar 周期 | 最少历史 Bar |
| --- | --- | --- | --- |
| `moving_average_cross` | 基础 SMA 双均线交叉 | 1 分钟 | `long_window + 1` |
| `moving_average_cross_5s` | 基础 SMA 双均线交叉 | 5 秒 | `long_window + 1` |
| `moving_average_cross_v2` | 带确认、冷却、波动和趋势过滤的双均线策略 | 1 分钟或 5 秒 | `max(long_window, atr_window + 1, trend_window) + confirmation_bars + cooldown_bars` |
| `close_threshold` | 按收盘价上下阈值产生信号 | 1 分钟 | 1 |
| `paper_round_trip` | paper 环境全链路验证 | 1 分钟 | 1 |

运行以下命令可查看当前二进制实际注册的策略类型：

```bash
quant strategy kinds
```

## 2. 公共运行模型

### 2.1 输入

每次评估接收按时间升序排列的、已经完成的 `StrategyBar`：

- `time`：Bar 时间（UTC）；
- `open`、`high`、`low`、`close`：OHLC；
- `volume`：成交量。实时运行中当前实际写入的是聚合 Tick 数。

策略只在新 final Bar 到达时运行，同一策略、同一 Bar 最多持久化一次评估结果。
1 分钟策略读取 `market_minute_bars`，5 秒策略读取
`market_five_second_bars`。没有成交 Tick 的 5 秒区间不会生成空 Bar。

### 2.2 输出

所有策略统一返回：

- `signal`：`buy`、`sell` 或 `hold`；
- `indicator_a`、`indicator_b`：当前两个主要指标；
- `previous_indicator_a`、`previous_indicator_b`：相应前值；
- `details`：策略专属 JSON 诊断内容。

四个标量指标用于高效 SQL 查询，`details` 用于审计信号原因及计算上下文。策略仅
生成方向信号，不决定下单数量，也不直接访问 IBKR、数据库、网络或系统时间。

### 2.3 实时与回测语义

实时运行和回测都通过 `strategy::build` 创建策略并调用 `evaluate()`。回测采用
“当前 Bar 收盘后产生信号，最早在下一根 Bar 开盘成交”，并计入配置的滑点和佣金。
当前回测执行模型只维护多头仓位：

- 空仓收到 `buy` 后，下一根 Bar 尝试买入；
- 持有多头收到 `sell` 后，下一根 Bar 尝试卖出；
- 空仓的 `sell` 不开空仓；
- 已持仓时的重复 `buy` 不加仓。

回测所选 timeframe 应与策略的 Bar 周期一致；策略配置中的 `conid` 必须与回测请求
的 `conid` 一致。

通过 `strategy_id` 回测已保存策略时，后端直接从策略记录解析 kind、config、conid
和 Bar 周期，调用方不能替换证券或周期。Web 也只展示这些锁定字段。只有不引用已有
策略的临时/通用回测请求才显式提供证券和配置。Parquet 历史下载和回测支持真实
`5s` Bar；下载任务按每小时分片请求 IBKR，不能用 1 分钟或其他周期代替。

`commission_per_order` 是用户提供的每笔固定金额，并不会按交易所、成交额、最低
收费、印花税或平台费自动计算。它与实时绩效中的 IBKR 实际 `CommissionReport`
不是同一数据源。研究非美国市场或高换手策略时，必须先把完整费用折算成保守的每笔
估算，并用 paper 成交报告校准。

## 3. `moving_average_cross`

基础 1 分钟简单移动平均线交叉策略。

### 参数

```json
{
  "conid": 756733,
  "short_window": 5,
  "long_window": 20
}
```

| 参数 | 含义 | 约束 |
| --- | --- | --- |
| `conid` | IBKR 合约 ID | `> 0` |
| `short_window` | 短期 SMA 窗口 | `> 0` |
| `long_window` | 长期 SMA 窗口 | `short_window < long_window <= 10000` |

### 实现与信号

对收盘价计算当前和前一根 Bar 时点的简单移动平均：

```text
SMA(values) = sum(values) / values.length
```

- `buy`：前一时点 `short_sma <= long_sma`，当前
  `short_sma > long_sma`；
- `sell`：前一时点 `short_sma >= long_sma`，当前
  `short_sma < long_sma`；
- 其他情况：`hold`。

因此策略只在实际穿越发生的 Bar 发出买卖信号，不会在短均线持续位于长均线上方或
下方时重复发出同方向信号。最少需要 `long_window + 1` 根 final Bar，以便同时计算
当前值和前值。

主要输出为短、长 SMA 及其前值；`details` 还包含 timeframe、窗口参数和当前 Bar
的 OHLCV。

### 创建

```bash
quant strategy create-ma \
  --name spy-ma-5-20 \
  --conid 756733 \
  --short-window 5 \
  --long-window 20
```

也可使用通用接口：

```bash
quant strategy create \
  --name spy-ma-5-20 \
  --kind moving_average_cross \
  --config-json '{"conid":756733,"short_window":5,"long_window":20}'
```

## 4. `moving_average_cross_5s`

5 秒版基础双均线交叉策略。参数校验、SMA 公式和买卖条件与
`moving_average_cross` 完全相同，唯一差异是读取已经完成的 5 秒 Bar，并在
`details.timeframe` 中记录 `5s`。

```bash
quant strategy create \
  --name spy-ma-5s-5-20 \
  --kind moving_average_cross_5s \
  --config-json '{"conid":756733,"short_window":5,"long_window":20}'
```

该策略依赖连续的实时成交 Tick。`short_window = 5`、`long_window = 20` 表示 5
根和 20 根实际生成的 5 秒 Bar，而不是固定的 25 秒和 100 秒墙钟时间。

## 5. `moving_average_cross_v2`

V2 是抗噪声双均线策略，在均线方向之外加入均线差、ATR、长期趋势、连续确认和信号
冷却。它仍只产生方向信号，仓位管理与订单执行留在策略外部。

### 参数

```json
{
  "conid": 756733,
  "short_window": 5,
  "long_window": 20,
  "bar_timeframe": "1m",
  "average_type": "ema",
  "min_gap_percent": 0.05,
  "confirmation_bars": 2,
  "cooldown_bars": 3,
  "atr_window": 14,
  "min_atr_percent": 0.0,
  "trend_window": 0
}
```

| 参数 | 含义 | 默认值 | 约束 |
| --- | --- | --- | --- |
| `conid` | IBKR 合约 ID | 无 | `> 0` |
| `short_window` | 短均线窗口 | 无 | `> 0` |
| `long_window` | 长均线窗口 | 无 | `short_window < long_window <= 10000` |
| `bar_timeframe` | 实时 Bar 周期 | `"1m"` | `"1m"` 或 `"5s"` |
| `average_type` | 均线算法 | `"ema"` | `"sma"` 或 `"ema"` |
| `min_gap_percent` | 最小均线差占价格百分比 | `0` | `0..=100` |
| `confirmation_bars` | 方向连续成立多少根后发信号 | `2` | `1..=1000` |
| `cooldown_bars` | 两次新信号之间的冷却窗口 | `0` | `0..=10000` |
| `atr_window` | ATR 窗口 | `14` | `1..=10000` |
| `min_atr_percent` | 最小 ATR 占价格百分比 | `0` | `0..=100` |
| `trend_window` | 长期价格趋势均线窗口；0 为关闭 | `0` | `0..=10000` |

两个百分比参数使用百分数单位。例如 `0.05` 表示 `0.05%`，不是 `5%`。

### 指标计算

SMA 与基础版相同。EMA 在所选窗口内以第一个值为初始 EMA，并按以下公式递推：

```text
alpha = 2 / (window + 1)
EMA(t) = alpha * close(t) + (1 - alpha) * EMA(t-1)
```

均线差和 ATR 百分比为：

```text
gap_percent = abs(short_ma - long_ma) / close * 100
true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
ATR = average(true_range over atr_window)
atr_percent = ATR / close * 100
```

当 `close <= 0` 时两个百分比按 0 处理。

### 候选方向与过滤

先要求：

```text
gap_percent >= min_gap_percent
atr_percent >= min_atr_percent
```

通过后得到候选方向：

- 多头候选：`short_ma > long_ma`，并且趋势过滤关闭，或
  `close >= trend_average`；
- 空头候选：`short_ma < long_ma`，并且趋势过滤关闭，或
  `close <= trend_average`；
- 其他情况：无方向。

这里的 `trend_average` 使用与主均线相同的 `average_type`。

### 确认、触发和冷却

策略逐根计算候选方向：

- 方向连续相同则累计 streak；
- 方向变化则以新方向重新从 1 计数；
- 无方向则清零；
- 仅当 streak **恰好达到** `confirmation_bars` 的当前 Bar 才形成一次候选信号，
  持续满足方向不会每根 Bar 重复触发；
- 若最近一次历史候选信号距当前不超过 `cooldown_bars`，当前信号被抑制为
  `hold`。

候选方向为多头时发 `buy`，为空头时发 `sell`，其余情况发 `hold`。为确保实时运行
与使用扩展历史的回测结果一致，V2 每次只使用末尾 `minimum_history()` 根 Bar。

`details.signal_reason` 可能为：

| 值 | 含义 |
| --- | --- |
| `confirmed_cross` | 方向通过过滤并完成连续确认 |
| `cooldown` | 候选信号处于冷却期 |
| `gap_below_threshold` | 均线差不足 |
| `atr_below_threshold` | 波动率不足 |
| `trend_filter` | 未通过长期趋势过滤 |
| `waiting_for_confirmation_or_new_cross` | 正在等待连续确认或新的方向触发 |

诊断 JSON 还保存所有配置、当前短长均线、均线差、ATR、ATR 百分比、趋势均线、
`qualified_direction` 和当前 Bar。

### 创建

```bash
quant strategy create \
  --name spy-ma-v2 \
  --kind moving_average_cross_v2 \
  --config-json '{
    "conid":756733,
    "short_window":5,
    "long_window":20,
    "bar_timeframe":"1m",
    "average_type":"ema",
    "min_gap_percent":0.05,
    "confirmation_bars":2,
    "cooldown_bars":3,
    "atr_window":14,
    "min_atr_percent":0,
    "trend_window":0
  }'
```

Web 的“均线策略向导”也支持创建 V2，并默认给出上述示例参数。

## 6. `close_threshold`

一个简单的收盘价阈值策略，也用作新增策略的参考实现。

### 参数与校验

```json
{
  "conid": 756733,
  "buy_below": 600.0,
  "sell_above": 800.0
}
```

要求 `conid > 0`，两个阈值均为有限数，并满足：

```text
0 < buy_below < sell_above
```

### 信号

- `close < buy_below`：`buy`；
- `close > sell_above`：`sell`；
- 包含两个阈值本身在内的中间区间：`hold`。

它只需要最新 1 根 final 1 分钟 Bar。与交叉策略不同，只要价格仍在阈值之外，它就会
在每个新 Bar 重复产生相同方向信号；执行层的目标仓位和活动订单检查负责避免无意义
的重复下单。

```bash
quant strategy create \
  --name spy-threshold \
  --kind close_threshold \
  --config-json '{"conid":756733,"buy_below":600,"sell_above":800}'
```

## 7. `paper_round_trip`

该策略专门用于验证 signal → action → order → execution → position →
performance 全链路，不是盈利策略，不应用于 live。

### 参数

```json
{
  "conid": 756733,
  "phase_bars": 1
}
```

`phase_bars` 默认 1，允许范围为 `1..=1440`；`conid` 必须大于 0。

### 实现

策略按 final Bar 的 UTC Unix 分钟确定 phase：

```text
minute = floor(unix_timestamp / 60)
phase = floor(minute / phase_bars)
偶数 phase -> buy
奇数 phase -> sell
```

因此 `phase_bars = 1` 时按 UTC epoch 分钟奇偶交替；更大的值会让每个方向持续多个
分钟。信号取决于绝对 UTC 时间，而不是策略启动后累计了多少根 Bar。策略默认使用 1
分钟 Bar，只需 1 根历史数据。

建议将 `buy` 映射为一个很小的多头目标仓位，将 `sell` 映射为 0，从而在 paper
账户中产生往返交易：

```bash
quant instrument search <SYMBOL>

quant strategy create \
  --name paper-web-round-trip \
  --kind paper_round_trip \
  --config-json '{"conid":<SEARCHED_CONID>,"phase_bars":1}'
```

必须使用当前 IB Gateway 搜索得到的真实合约，确认 `conid > 0`、
`security_type = STK`，并先取得新鲜的实时 Bid/Ask。当前自动执行层不支持 `CASH`
外汇，Delayed 行情也会被拒绝。

## 8. 策略生命周期与查看信号

```bash
quant strategy list
quant strategy start <STRATEGY_ID>
quant strategy pause <STRATEGY_ID>
quant strategy stop <STRATEGY_ID>
quant strategy signals <STRATEGY_ID> --limit 100
```

创建策略不会自动下单。策略启动后只评估 final Bar 并持久化信号；订单执行必须单独
配置和显式启用。

## 9. 回测所有注册策略

通用回测入口可以运行任意注册策略：

```bash
quant backtest run-strategy \
  --conid 756733 \
  --timeframe 1m \
  --start 2026-01-01T00:00:00Z \
  --end 2026-07-01T00:00:00Z \
  --kind moving_average_cross_v2 \
  --config-json '{
    "conid":756733,
    "short_window":5,
    "long_window":20,
    "bar_timeframe":"1m",
    "average_type":"ema",
    "min_gap_percent":0.05,
    "confirmation_bars":2,
    "cooldown_bars":3,
    "atr_window":14,
    "min_atr_percent":0,
    "trend_window":0
  }' \
  --quantity 1 \
  --slippage-bps 5 \
  --commission-per-order 1
```

除旧版 `moving_average_cross` 的兼容回测入口外，其他类型必须提供完整
`strategy_config`。数据至少需要 `minimum_history + 1` 根 Bar，额外一根用于下一根
开盘成交语义。

所有均线策略本身都不预测收益；`min_gap_percent` 只描述均线之间的价格差比例。
可选的执行成本门控会用 `abs(indicator_a-indicator_b)/close` 作为统一信号强度代理，
与数据库费用模型估计的完整往返成本比较。它能过滤明显无法覆盖费用的信号，但不是
盈利预测。5 秒策略尤其容易产生高换手。

## 10. Paper 自动执行

执行器采用目标仓位语义：

- `buy`：将每条腿调整到 `buy_target_quantity`；
- `sell`：将每条腿调整到 `sell_target_quantity`，单标的默认是 0；
- `hold`：不创建 action；
- 当前仓位已经等于目标时不下单；
- 同一合约存在活动订单时跳过；
- 每个 evaluation 只有一个持久化 action 和幂等键；
- action 在 `processing` 时崩溃会标记失败并要求人工对账，不会盲目重发。

单标的配置：

```bash
quant strategy execution configure \
  --strategy-id <STRATEGY_ID> \
  --account DU123456 \
  --target-quantity 1 \
  --conid 756733 \
  --symbol SPY \
  --primary-exchange ARCA \
  --local-symbol SPY

quant strategy execution enable <STRATEGY_ID> --confirm
quant strategy execution actions --limit 100
quant strategy execution disable <STRATEGY_ID>
```

自动执行默认提交市价单且 `outside_rth = false`。执行配置开启盘前盘后后，订单改用
限价单并设置 `outside_rth = true`；买入限价取最新 Ask，卖出限价取最新 Bid。
下单前会从 IBKR `ContractDetails` 自动维护交易日历：正常订单检查 `liquidHours`，
盘前盘后订单检查 `tradingHours`。时段按 IBKR 的 `timeZoneId` 处理夏令时并转换为
UTC，缓存每 6 小时按需刷新，同一交易日的分段交易会分别保存。日历无法获取或当前
交易所不在相应时段时，自动执行保持关闭并记录具体原因。
策略信号计算与自动执行开关相互独立。执行配置关闭期间仍会保存 `buy`/`sell` 信号，
但系统会同时写入一条 `skipped` action，成本门控结果标记为
`execution_disabled`，明确说明该信号没有进入下单流程；重新启用后不会补交历史信号。
扩展时段流动性不足时订单可能不会立即成交。启用要求：

- 运行环境为 `paper`；
- `[risk].trading_enabled = true`；
- execution config 的 `paper_only = true`；
- 命令显式提供 `--confirm`；
- 合约拥有新鲜的实时 Bid/Ask。

信号转换出的订单仍走标准 `order.submit`，必须通过账户、行情新鲜度、持仓、订单
频率、敞口、PnL、对账和紧急停止等全部风控。live 自动策略执行被硬性禁止。

如果策略在 Web“交易成本”页面启用了成本控制，执行器会在 `order.submit` 前估算：

```text
单边佣金 = max(最低收费, 每笔固定费 + 数量 × 每股费 + 名义金额 × 比例费率)
完整成本 = 买入佣金 + 卖出佣金/税费 + 点差 + 双边滑点
所需强度 = 完整成本 / 名义金额 × 10000 × 安全倍数
```

配置估算还会与该策略历史实际佣金有效 bps 的 P90 取更保守值。信号强度缺失或低于
门槛时 action 记为 `skipped`。最新绩效快照达到最少交易数且佣金/毛利润超过配置
上限时，执行配置自动停用。

订单状态会记录剩余数量、最近成交价、`why_held`、market-cap price，并将 IBKR
open/completed order 状态、拒绝原因和警告写入 `broker_order_events`。排查未成交
订单时，应把这些字段与实时 bid/ask、交易时段及活动订单检查一起查看。

做空目标需要同时显式配置负目标与 `--allow-short`：

```bash
quant strategy execution configure \
  --strategy-id <STRATEGY_ID> \
  --account DU123456 \
  --target-quantity 100 \
  --short-target-quantity -100 \
  --allow-short \
  --conid 756733 \
  --symbol SPY \
  --primary-exchange ARCA
```

多腿组合可用 `strategy execution configure-portfolio` 分别声明各腿在 `buy` 和
`sell` 下的目标仓位。系统会先对所有腿完成实时 Bid/Ask 和交易日历预检，再逐腿
提交；IBKR 不提供跨股票原子成交，中途出现未知结果时系统会停止并要求人工对账。

## 11. 新增策略

策略实现必须满足：

- 确定性、无副作用；
- 输入仅为按时间升序排列的 final Bar；
- 不直接访问 IBKR、DuckDB、网络或系统时间；
- 不直接下单；
- 相同配置与 Bar 必须产生相同输出；
- `minimum_history()` 准确声明所需最少 Bar；
- `bar_timeframe()` 只能返回实时运行器支持的 `"1m"` 或 `"5s"`。

新增流程：

1. 在 `src/strategy.rs` 定义可序列化配置并完成参数校验；
2. 实现 `Strategy` trait；
3. 在 `strategy::build` 增加 factory 分支；
4. 将名称加入 `registered_kinds()`；
5. 为参数边界、`buy`、`sell`、`hold` 和最少历史数据添加单元测试；
6. 执行验证：

```bash
cargo fmt --all
cargo test
cargo check
```

注册完成后，daemon、JSON-RPC 实时运行器和通用回测引擎会自动共享该实现。
