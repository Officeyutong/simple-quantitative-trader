# 策略说明

本文档按当前源码整理项目中所有已注册策略。公共接口位于
`crates/strategy-api`，每个策略族分别拥有 `model`、`engine` 和 `web` crate；
前后端 Catalog 负责静态注册。`src/strategy.rs` 仅是主程序兼容门面。实时运行器与
回测引擎通过同一个 `Strategy` trait 调用同一份信号逻辑。

## 1. 策略总览

| `kind` | 用途 | Bar 周期 | 最少历史 Bar |
| --- | --- | --- | --- |
| `moving_average_cross` | 基础 SMA 双均线交叉 | 1 分钟 | `long_window + 1` |
| `moving_average_cross_5s` | 基础 SMA 双均线交叉 | 5 秒 | `long_window + 1` |
| `moving_average_cross_v2` | 带等待确认、冷却、波动和趋势过滤的双均线策略 | 1 分钟或 5 秒 | `max(long_window, atr_window + 1, trend_window) + confirmation_window_bars + cooldown_bars` |
| `close_threshold` | 按收盘价上下阈值产生信号 | 1 分钟 | 1 |
| `bollinger_rsi_mean_reversion` | 布林带与 RSI 联合确认的多头均值回归 | 1 分钟或 5 秒 | `max(bollinger_window + 1, rsi_window + 2)` |
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
`market_five_second_bars`。Bar 按固定墙钟时间桶聚合；一个 Bar 只有在更晚时间桶的
首个成交 Tick 到达时才被定稿（final），因此交易时段的最后一根 Bar 会等到下一时段
的首个 Tick 才参与评估。较短的无成交间隔（最多 120 个区间，5 秒表约 10 分钟、
1 分钟表约 2 小时）会以上一根收盘价合成 `tick_count = 0` 的平盘 final Bar，保证
N 根 Bar 的指标仍然对应 N 个固定时间桶；更长的中断不合成 Bar，评估会因窗口不连续
暂停，直到累积足够的连续新 Bar。已定稿的 Bar 不可变：迟到的乱序 Tick 不会改写
final Bar 的 OHLC。

### 2.2 输出

所有策略统一返回：

- `signal`：`buy`、`sell` 或 `hold`；
- `indicator_a`、`indicator_b`：当前两个主要指标；
- `previous_indicator_a`、`previous_indicator_b`：相应前值。均线类策略填前一根
  Bar 的均线值；`close_threshold` 和 `paper_round_trip` 借用这些字段保存阈值或
  相位等参照值（详见各策略章节），并非严格意义上的“前值”；
- `details`：策略专属 JSON 诊断内容。

四个标量指标用于高效 SQL 查询，`details` 用于审计信号原因及计算上下文。策略仅
生成方向信号，不决定下单数量，也不直接访问 IBKR、数据库、网络或系统时间。

### 2.3 持久化运行状态

有状态策略通过 `Strategy::initial_state()` 提供初始 JSON，通过
`evaluate_with_state(bars, state)` 返回 `StrategyTransition`：

- `output`：本次可审计信号和指标；
- `next_state`：处理本 Bar 后应持久化的完整状态；
- `state_version()`：状态 schema 版本，默认是 1。

平台将状态保存在 `strategy_runtime_states`，包含 `state_json`、`state_version`、
单调递增的 `revision` 和 `last_transition_bar`。evaluation、`last_evaluated_bar`
和下一状态在同一个 DuckDB 事务中提交，因此不会出现信号已保存但状态未前进，或
状态已前进但信号丢失的情况。daemon 重启后从该表恢复状态。

状态默认最大 1 MiB。版本与当前 engine 不匹配时策略会失败关闭并把原因写入
`last_error`，不会静默丢弃或错误解释旧状态。升级有状态策略时必须先提供明确的状态
迁移或重置流程。普通无状态策略无需改动：默认状态为 `{}`，默认状态转换保持其原值。
回测使用相同的状态转换接口，但状态只存在于该次回测内，不写入实时策略状态表。

### 2.4 实时与回测语义

实时运行和回测都通过 `strategy::build` 创建策略并调用 `evaluate()`。回测采用
“当前 Bar 收盘后产生信号，最早在下一根 Bar 开盘成交”，并按数据库费用模型计入
佣金、税费、点差和滑点。
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

回测不会再把“存在任意重叠 Parquet 文件”当成完整数据。历史下载任务的 cursor 只在
IBKR 请求成功且分片落盘后推进；系统合并匹配 `conid`、Bar 周期和 `outside_rth` 的
成功抓取区间，只有其完整覆盖回测请求并且范围内存在 Bar 时才允许运行。该校验同时在
Web 和后端执行，直接调用 RPC 也不能绕过。自然时间 `raw_gaps` 仅用于诊断，因为夜间、
周末和休市时段本来就不应产生 Bar。

历史任务队列会按 `conid`、周期和 `outside_rth` 合并重叠的活动请求，避免移动的结束
时间或重复点击产生整段重复下载。Web 使用后端计算的有效运行状态和队列位置，区分
“正在下载”“排队中”和“等待 IBKR 就绪”，并显示当前真正占用单 worker 的任务。

通过 `strategy_id` 运行时，回测强制使用该策略在“交易成本”页面绑定的数据库费用
模型，即使实时成本门控处于关闭状态也照常扣费。临时/通用回测必须显式提供数据库
`cost_model_id`，不再接受独立的每单佣金或滑点参数。模型币种必须与证券币种一致。

买卖每一腿都使用与实时成本门控相同的确定性公式：

```text
券商费 = max(最低费, 固定费 + 数量 × 每股费 + 名义金额 × 比例费率)
卖出佣金/税费 = 卖出券商费 + 名义金额 × 卖出税率
单腿点差成本 = 名义金额 × 完整点差 bps / 2
单腿滑点成本 = 名义金额 × 单边滑点 bps
```

点差与滑点按方向调整下一根 Bar 的开盘成交价，佣金和税费直接从现金扣除。回测结果
分别保存佣金/税费、点差、滑点和总执行成本，并把完整费用模型写入参数快照；之后编辑
数据库模型不会改变历史回测。实时成交后的绩效仍以 IBKR 实际
`CommissionReport` 为准，用于校准估计模型。

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
根和 20 根 5 秒 Bar。短暂无成交的区间会按 §2.1 的规则以前收合成平盘 Bar，因此
在合成范围内窗口对应固定墙钟时间；超过合成上限的长中断会使评估暂停等待连续
历史。

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
  "confirmation_window_bars": 12,
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
| `confirmation_window_bars` | 交叉后允许等待过滤条件达标的最大 Bar 数；包含交叉 Bar | `12` | `confirmation_bars..=10000` |
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

确认逻辑以“真实均线交叉”为锚点，并为过滤条件提供有限的达标时间：

- 设当前评估位置的原始均线方向（不含过滤）为 `direction`（`short > long` 为
  多头、`short < long` 为空头、相等为无方向）；
- 原始方向从明确的反方向切换时创建一个待确认交叉；均线完全相等不创建交叉；
- 待确认交叉最多保留 `confirmation_window_bars` 根 Bar。在此期间，均线差、ATR
  或趋势过滤可以稍后达标；过滤未通过会把连续确认进度重置为 0，但不会立即丢弃
  交叉；
- 过滤后方向连续 `confirmation_bars` 根等于交叉方向时发出一次信号；
- 等待期间若发生反向交叉，旧候选立即取消并以新方向重新开始；若等待窗口耗尽仍
  未完成确认，候选过期；
- 信号发出后候选被消费，方向持续成立不会在后续 Bar 重复触发；
- 若同一窗口内更早的位置也曾满足触发条件，且距当前不超过 `cooldown_bars`，
  当前信号被抑制为 `hold`。

因此，交叉发生时均线差尚小于阈值不会永久丢失信号；只要在等待窗口内达标并完成
连续确认，仍可触发。等待窗口限制了陈旧交叉，避免在很久以后才追涨或追跌。

候选方向为多头时发 `buy`，为空头时发 `sell`，其余情况发 `hold`。为确保实时运行
与使用扩展历史的回测结果一致，V2 每次只使用末尾 `minimum_history()` 根 Bar。

`details.signal_reason` 可能为：

| 值 | 含义 |
| --- | --- |
| `confirmed_cross` | 方向通过过滤并完成连续确认 |
| `cooldown` | 候选信号处于冷却期 |
| `gap_below_threshold` | 均线差不足 |
| `atr_below_threshold` | 波动率不足 |
| `trend_filter` | 过滤后无方向且趋势过滤开启（含均线无方向、未通过趋势过滤两种情况） |
| `waiting_for_confirmation` | 已有待确认交叉，正在等待过滤条件和连续确认 |
| `confirmation_window_expired` | 待确认交叉未能在窗口内完成确认，已经过期 |
| `waiting_for_new_cross` | 当前没有有效的待确认交叉 |

诊断 JSON 还保存所有配置、当前短长均线、均线差、ATR、ATR 百分比、趋势均线、
`qualified_direction`、`pending_direction`、`confirmation_progress`、
`confirmation_window_remaining` 和当前 Bar。

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
    "confirmation_window_bars":12,
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

指标字段：`indicator_a` 为当前收盘价，`previous_indicator_a` 也填当前收盘价，
`indicator_b`/`previous_indicator_b` 分别保存 `buy_below` 与 `sell_above` 阈值，
便于 SQL 直接对照信号与阈值。

```bash
quant strategy create \
  --name spy-threshold \
  --kind close_threshold \
  --config-json '{"conid":756733,"buy_below":600,"sell_above":800}'
```

## 7. `bollinger_rsi_mean_reversion`

这是一个多头均值回归策略。价格首次跌破布林带下轨且滚动 RSI 进入超卖区时买入；
价格重新上穿中轨或 RSI 恢复到退出阈值时卖出。它不会把上轨信号解释为开空，因此
`supports_short_targets = false`。

### 参数

```json
{
  "conid": 756733,
  "bar_timeframe": "1m",
  "bollinger_window": 20,
  "standard_deviations": 2.0,
  "rsi_window": 14,
  "oversold_rsi": 30.0,
  "exit_rsi": 50.0,
  "minimum_bandwidth_percent": 0.0
}
```

| 参数 | 含义 | 默认值 | 约束 |
| --- | --- | --- | --- |
| `conid` | IBKR 合约 ID | 无 | `> 0` |
| `bar_timeframe` | Bar 周期 | `"1m"` | `"1m"` 或 `"5s"` |
| `bollinger_window` | 布林带窗口 | `20` | `2..=10000` |
| `standard_deviations` | 上下轨标准差倍数 | `2` | `0 < value <= 100` |
| `rsi_window` | RSI 涨跌统计窗口 | `14` | `2..=10000` |
| `oversold_rsi` | 买入超卖阈值 | `30` | `0 <= value < exit_rsi` |
| `exit_rsi` | 均值修复退出阈值 | `50` | `oversold_rsi < value <= 100` |
| `minimum_bandwidth_percent` | 最小布林带宽度占中轨百分比 | `0` | `0..=100`，0 为关闭 |

布林带使用总体标准差：

```text
middle = average(close over bollinger_window)
sigma = sqrt(sum((close - middle)^2) / bollinger_window)
lower = middle - standard_deviations * sigma
upper = middle + standard_deviations * sigma
bandwidth_percent = (upper - lower) / abs(middle) * 100
```

RSI 使用固定滚动窗口内上涨幅度与下跌幅度总和（Cutler RSI），而不是依赖无限历史的
Wilder 递推。全窗口完全不动时 RSI 定义为 50；只有上涨时为 100；只有下跌时为 0。
这样实时运行、daemon 重启和有限历史回测始终具有完全相同的结果。

### 信号

- `buy`：当前 `close < lower`、`RSI <= oversold_rsi` 且带宽合格，并且上一根 Bar
  尚未同时满足这三个条件；
- `sell`：价格本 Bar 首次重新达到中轨，或 RSI 本 Bar 首次达到 `exit_rsi`；
- `hold`：其他情况。超卖条件持续成立时不会每根 Bar 重复产生买入信号。

策略把当前收盘价和布林中轨分别写入 `indicator_a`、`indicator_b`，因此现有成本门控
使用“当前价到预期回归均值的距离”作为信号强度，而不是只使用跌破下轨的微小距离。
`details` 额外保存上下轨、中轨、当前/前一 RSI、带宽、条件布尔值和
`signal_reason`。

```bash
quant strategy create \
  --name msft-bollinger-rsi \
  --kind bollinger_rsi_mean_reversion \
  --config-json '{
    "conid":272093,
    "bar_timeframe":"1m",
    "bollinger_window":20,
    "standard_deviations":2,
    "rsi_window":14,
    "oversold_rsi":30,
    "exit_rsi":50,
    "minimum_bandwidth_percent":0
  }'
```

Web“均值回归向导”提供证券搜索和同一组参数表单。向导只创建停止状态的策略，不会
隐式开启信号或自动执行；创建后仍需分别配置执行目标、费用模型和历史数据。

## 8. `paper_round_trip`

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
分钟 Bar，只需 1 根历史数据。指标字段保存当前与前一 phase 序号，仅用于诊断。

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

## 9. 策略生命周期与查看信号

```bash
quant strategy list
quant strategy start <STRATEGY_ID>
quant strategy pause <STRATEGY_ID>
quant strategy stop <STRATEGY_ID>
quant strategy signals <STRATEGY_ID> --limit 100
```

创建策略不会自动下单。策略启动后只评估 final Bar 并持久化信号；订单执行必须单独
配置和显式启用。

## 10. 回测所有注册策略

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
    "confirmation_window_bars":12,
    "cooldown_bars":3,
    "atr_window":14,
    "min_atr_percent":0,
    "trend_window":0
  }' \
  --quantity 1 \
  --cost-model-id <COST_MODEL_ID>
```

除旧版 `moving_average_cross` 的兼容回测入口外，其他类型必须提供完整
`strategy_config`。数据至少需要 `minimum_history + 1` 根 Bar，额外一根用于下一根
开盘成交语义；整个请求范围还必须具有完整的成功抓取证明，不能用局部数据冒充长范围
回测。

所有均线策略本身都不预测收益；`min_gap_percent` 只描述均线之间的价格差比例。
可选的执行成本门控会把信号强度换算为 bps，与数据库费用模型估计的完整往返成本
比较。信号强度按策略取值：均线策略使用 `abs(indicator_a-indicator_b)/close`；
`close_threshold` 使用收盘价与对应方向阈值（买入用 `buy_below`，卖出用
`sell_above`）的距离；`paper_round_trip` 不提供信号强度，不参与成本门控。
它能过滤明显无法覆盖费用的信号，但不是盈利预测。5 秒策略尤其容易产生高换手。
注意通用回测入口不校验 `--timeframe` 与策略 `bar_timeframe` 是否一致，需自行
保证；按 `strategy_id` 运行的回测会强制锁定保存配置中的证券与周期。

## 11. Paper 自动执行

执行器采用目标仓位语义：

- `buy`：将每条腿调整到 `buy_target_quantity`；
- `sell`：将每条腿调整到 `sell_target_quantity`，单标的默认是 0；
- `hold`：不创建 action；
- 当前仓位已经等于目标时不下单；
- 同一合约存在活动订单，或存在提交中（`approved`）/结果不明（`unknown`）的
  订单意图时跳过，`unknown` 意图必须先通过对账或 `order.intent.resolve`
  人工确认结果；
- IBKR 持仓快照正在同步（`syncing`）期间不认领信号，等待快照就绪后按当时的
  真实仓位计算目标差量；
- 信号从生成起 15 分钟内未被执行（例如 daemon 停机期间积压）会被记录为
  `skipped`（`stale_signal`），永不补交；
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
上限时，执行配置自动停用。该熔断只在累计毛利润为正时计算；毛利润为零或负数不再
换算成无穷比例并触发成本熔断，而应由最大回撤、连续亏损等独立风险规则判断。

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
- 有内部状态时，覆盖 `initial_state()`、`state_version()` 和
  `evaluate_with_state()`；不得在策略对象或全局变量中隐藏跨 Bar 状态。

每个策略族的目录约定如下：

```text
strategies/<strategy>/
├── model/   # 配置类型、字段 schema、显示名称和能力声明
├── engine/  # Strategy trait 实现、参数校验和后端 factory
└── web/     # 参数展示及可选的专用 Yew 表单/向导
```

共享边界：

- `strategy-api` 只包含 Bar、信号、trait、配置字段 schema 和注册项，不依赖主程序；
- `strategy-web-kit` 提供 schema 驱动的通用表单与参数展示；
- `strategy-catalog-backend` 是 daemon/回测使用的后端注册表；
- `strategy-catalog-web` 是 Yew 使用的前端注册表；
- Broker、数据库、风险、成本门控和订单执行仍属于平台，不进入策略 crate。

新增流程：

1. 创建 `<strategy>-model`，定义可序列化配置、字段说明和 `StrategyMetadata`；
2. 创建 `<strategy>-engine`，实现 `Strategy`、参数校验和
   `BackendStrategyRegistration`；
3. 创建 `<strategy>-web`，通常复用 `GenericStrategyForm` 和
   `render_config_table`；有特殊交互时在该 crate 内提供专用组件；
4. 分别向 `strategy-catalog-backend` 与 `strategy-catalog-web` 各增加一个注册项；
5. 将三个 crate 加入 workspace。主程序、实时运行器、回测和策略状态页不需要再增加
   `kind` 分支；
6. 为参数边界、`buy`、`sell`、`hold` 和最少历史数据添加单元测试；
7. 执行验证：

```bash
cargo fmt --all
cargo test --workspace
cargo check --workspace
cd web && trunk build --release
```

注册完成后，daemon、JSON-RPC 实时运行器和通用回测引擎会自动共享 engine；
`strategy.kinds` 会返回 `kinds` 兼容列表及带字段 schema/能力的 `strategies` 列表，
Web 策略状态页会通过 Web Catalog 展示对应参数。
