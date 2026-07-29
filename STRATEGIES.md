# 用 Rust 编写策略

策略核心位于 `src/strategy.rs`。实时运行器和回测引擎使用同一个 `Strategy`
trait，因此策略信号逻辑只实现一次。

## 1. 策略约束

策略必须是确定性、无副作用的纯计算代码：

- 输入仅包含按时间升序排列的 final Bar；
- 不直接访问 IBKR、DuckDB、网络或系统时间；
- 不直接下单；
- 相同配置和 Bar 必须产生相同输出；
- `minimum_history()` 必须准确声明最少 Bar 数。

订单执行、持仓、风控、对账和紧急停止由策略外部处理。

## 2. 实现参数和策略

下面是项目中已经注册的 `close_threshold` 示例：

```rust
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CloseThresholdConfig {
    pub conid: i32,
    pub buy_below: f64,
    pub sell_above: f64,
}

pub struct CloseThreshold {
    config: CloseThresholdConfig,
}

impl Strategy for CloseThreshold {
    fn kind(&self) -> &'static str {
        "close_threshold"
    }

    fn conid(&self) -> i32 {
        self.config.conid
    }

    fn minimum_history(&self) -> usize {
        1
    }

    fn evaluate(&self, bars: &[StrategyBar]) -> Result<StrategyOutput, String> {
        let bar = bars.last().ok_or("one final bar is required")?;
        let signal = if bar.close < self.config.buy_below {
            StrategySignal::Buy
        } else if bar.close > self.config.sell_above {
            StrategySignal::Sell
        } else {
            StrategySignal::Hold
        };
        Ok(StrategyOutput {
            signal,
            indicator_a: bar.close,
            indicator_b: self.config.buy_below,
            previous_indicator_a: bar.close,
            previous_indicator_b: self.config.sell_above,
            details: serde_json::json!({
                "close": bar.close,
                "buy_below": self.config.buy_below,
                "sell_above": self.config.sell_above
            }),
        })
    }
}
```

`indicator_a`、`indicator_b` 及其前值用于高效 SQL 查询；`details` 用于保存任意
策略诊断数据。

## 3. 注册策略

在 `strategy::build` 中加入 factory 分支：

```rust
"close_threshold" => {
    let config: CloseThresholdConfig =
        serde_json::from_value(config).map_err(|error| error.to_string())?;
    Ok(Box::new(CloseThreshold::new(config)?))
}
```

同时把名称加入 `registered_kinds()`。这是新增策略唯一需要修改的公共注册点。

## 4. 测试和编译

为策略添加纯函数单元测试，然后执行：

```bash
cargo fmt --all
cargo test
cargo check
```

## 5. 创建和运行

重新启动 daemon 后查看编译进二进制的策略：

```bash
quant strategy kinds
```

创建通用策略：

```bash
quant strategy create \
  --name spy-threshold \
  --kind close_threshold \
  --config-json '{"conid":756733,"buy_below":600.0,"sell_above":800.0}'

quant strategy start <STRATEGY_ID>
quant strategy signals <STRATEGY_ID> --limit 100
```

## 6. 使用同一代码回测

```bash
quant backtest run-strategy \
  --conid 756733 \
  --timeframe 1d \
  --start 2026-01-01T00:00:00Z \
  --end 2026-07-01T00:00:00Z \
  --kind close_threshold \
  --config-json '{"conid":756733,"buy_below":600.0,"sell_above":800.0}' \
  --quantity 1 \
  --slippage-bps 5 \
  --commission-per-order 1
```

实时运行和回测都通过 `strategy::build` 构造策略并调用同一个 `evaluate()`。
回测信号在当前 Bar 收盘后产生，最早在下一根 Bar 开盘成交。

## 7. Paper/Web 验证策略

内置的 `paper_round_trip` 专用于验证 signal → action → order → execution →
position → performance 全链路。它根据 final minute bar 的 UTC epoch phase 确定性地
交替产生 `buy` 和 `sell`；`phase_bars = 1` 时每分钟切换一次，且同一根 bar 只评估
一次。它不是盈利策略，不应用于 live。

优先选择当前开放、账户有权限且能获得实时 Bid/Ask 的股票或 ETF。当前行情订阅和
风控执行层只支持 `STK`，不能使用 `CASH` 外汇。具体合约必须先通过当前 IB Gateway
搜索确认，禁止照抄未知 conid：

```bash
quant instrument search <SYMBOL>
quant strategy create \
  --name paper-web-round-trip \
  --kind paper_round_trip \
  --config-json '{"conid":<SEARCHED_CONID>,"phase_bars":1}'
```

搜索结果必须满足 `conid > 0` 且 `security_type = STK`。随后订阅同一个合约，并将
执行配置的 `buy` 目标设为一个很小的多头
仓位、`sell` 目标设为零。必须使用搜索结果原样填写 symbol、security type、
currency、exchange 和 local symbol。先确认 `market-data health` 有新鲜实时
Bid/Ask，再启动策略和启用 paper execution。网页中依次查看“策略/最近执行动作”、
“订单与成交”、“运行总览/账户持仓”和“策略绩效”。

## 8. 配置 paper 策略执行

执行器使用目标多头仓位：

- `buy`：买入差额，直到达到 `target_quantity`；
- `sell`：默认目标为 0；配置负目标时可反向做空；
- `hold`：不创建 action；
- 支持显式配置的负目标仓位和多腿目标组合；
- 同一合约存在活动订单时跳过；
- 每个 evaluation 只有一个持久化 action 和幂等键。
- daemon 在 `processing` 中崩溃时 action 标记为 failed 并要求人工核对，绝不盲目重发。

配置不会自动启用：

```bash
quant strategy execution configure \
  --strategy-id <STRATEGY_ID> \
  --account DU123456 \
  --target-quantity 1 \
  --conid 756733 \
  --symbol SPY \
  --primary-exchange ARCA \
  --local-symbol SPY

quant strategy execution list
```

单标的做空目标：

```bash
quant strategy execution configure \
  --strategy-id <STRATEGY_ID> --account DUR305382 \
  --target-quantity 100 --short-target-quantity -100 --allow-short \
  --conid 756733 --symbol SPY --primary-exchange ARCA
```

多标的组合使用 JSON 腿配置，每条腿分别声明 `buy`/`sell` 信号下的目标仓位：

```bash
quant strategy execution configure-portfolio \
  --strategy-id <STRATEGY_ID> --account DUR305382 \
  --legs-json '[
    {
      "contract":{"conid":441958876,"symbol":"3033","security_type":"STK",
        "currency":"HKD","exchange":"SEHK","primary_exchange":"SEHK",
        "local_symbol":"3033","description":"","derivative_security_types":[]},
      "buy_target_quantity":1000,"sell_target_quantity":0
    },
    {
      "contract":{"conid":445194075,"symbol":"3067","security_type":"STK",
        "currency":"HKD","exchange":"SEHK","primary_exchange":"SEHK",
        "local_symbol":"3067","description":"","derivative_security_types":[]},
      "buy_target_quantity":-500,"sell_target_quantity":0
    }
  ]'
```

组合先完成所有腿的实时 Bid/Ask 和日历预检，再逐腿提交。IBKR 不提供跨股票原子
成交；中途出现未知结果时系统不会盲目补偿，而会标记 failed 并要求对账。

确认配置、IBKR paper 连接、实时行情、对账和风险限制后才能启用：

```bash
quant strategy execution enable <STRATEGY_ID> --confirm
quant strategy execution actions --limit 100
quant strategy execution disable <STRATEGY_ID>
```

启用要求：

- 环境为 `paper`；
- `[risk].trading_enabled = true`；
- execution config 为 `paper_only = true`；
- CLI 提供 `--confirm`。

信号转换出的订单仍调用标准 `order.submit`，必须通过账户、行情新鲜度、持仓、
订单频率、敞口、PnL、对账、紧急停止等全部检查。

## 9. 安全边界

策略本身只生成 `buy`、`sell`、`hold`。只有单独配置并显式启用的 paper execution
config 才能把后续新信号转换成订单。自动执行只接受实时 Bid/Ask，Delayed tick
会被硬拒绝。live 自动策略执行仍被硬性禁止。
