# 构建与启动

本文面向第一次使用本项目的用户，说明如何构建 Web、让后端托管 Web 构建产物，
以及如何启动完整服务。以下命令默认在项目根目录执行。

## 1. 运行结构

项目启动后包含两个监听端口：

- Web HTTP：默认 `127.0.0.1:8080`，由后端直接提供 `web/dist` 中的静态文件；
- JSON-RPC WebSocket：默认 `127.0.0.1:8787`，Web 页面通过它读取状态和执行操作。

Web 构建完成后不需要另外运行 `trunk serve`。后端会读取 `[web].static_dir` 并同时
提供 Web 页面。

IB Gateway/TWS 是独立进程，必须启用 API，并与 `[ibkr]` 中的地址和端口一致。

## 2. 安装构建工具

需要：

- Rust stable 与 Cargo；
- `wasm32-unknown-unknown` Rust target；
- Trunk；
- C/C++ 构建工具。项目使用 bundled DuckDB，首次构建后端会花费较长时间。

已安装 Rustup 时执行：

```bash
rustup toolchain install stable
rustup default stable
rustup target add wasm32-unknown-unknown
cargo install trunk --locked
```

Debian/Ubuntu 可安装常用系统构建依赖：

```bash
sudo apt update
sudo apt install -y build-essential clang cmake pkg-config
```

确认工具可用：

```bash
rustc --version
cargo --version
trunk --version
```

## 3. 构建 Web

进入 Web 目录并执行 release 构建：

```bash
cd web
env -u NO_COLOR trunk build --release
cd ..
```

构建产物位于：

```text
web/dist/
```

至少应包含 `index.html`、生成的 WASM/JavaScript 文件和静态资源。

如果 Web 和浏览器都在本机运行，可以使用默认 RPC 地址
`ws://127.0.0.1:8787`。如果浏览器从另一台设备访问服务器，建议在构建时提供默认
RPC 地址：

```bash
cd web
QUANT_RPC_WS_URL=ws://192.168.1.20:8787 \
  env -u NO_COLOR trunk build --release
cd ..
```

将 `192.168.1.20` 替换为运行 daemon 的服务器地址。该值只是浏览器首次打开时的
默认值，之后可以在 Web 的“RPC 设置”页面修改；设置会保存在浏览器 LocalStorage。

## 4. 构建后端

在项目根目录执行：

```bash
cargo build --release
```

后端二进制位于：

```text
target/release/simple-quantitative-trader
```

如需在修改代码后执行完整检查：

```bash
cargo test
cargo build --release
cd web
env -u NO_COLOR trunk build --release
cd ..
```

只有 Web 代码发生变化时，无需重新构建后端；只有后端代码发生变化时，无需重新
构建 Web。

## 5. 创建配置文件

不要直接修改示例配置。先复制一份：

```bash
cp config/example.toml config/local.toml
```

本机访问的基本配置如下：

```toml
[app]
environment = "paper"
data_dir = "../data"
timezone = "UTC"

[rpc]
http_listen = "127.0.0.1:8787"
allowed_web_origin = "http://127.0.0.1:8080"

[web]
enabled = true
listen = "127.0.0.1:8080"
static_dir = "../web/dist"

[ibkr]
host = "127.0.0.1"
port = 4002
client_id = 17
connect_on_start = true
readonly = false

[risk]
trading_enabled = false
```

配置文件位于 `config/`，因此 `static_dir = "../web/dist"` 会解析为项目根目录下的
`web/dist`。如果配置文件放在其他目录，应相应调整相对路径，或者使用绝对路径。

首次安装建议保持：

```toml
[risk]
trading_enabled = false
```

确认行情、账户、对账和策略配置正确后，再在 Paper 环境中显式开启交易。即使全局
交易开关开启，每个策略的自动执行仍需单独配置和启用。

### 局域网访问

如果要从另一台电脑或手机访问，可配置：

```toml
[rpc]
http_listen = "0.0.0.0:8787"
allowed_web_origin = "http://192.168.1.20:8080"

[web]
enabled = true
listen = "0.0.0.0:8080"
static_dir = "../web/dist"
```

其中 `192.168.1.20` 是服务器的局域网地址。浏览器访问：

```text
http://192.168.1.20:8080
```

浏览器使用的 RPC 地址应为：

```text
ws://192.168.1.20:8787
```

`allowed_web_origin` 必须填写 Web 页面实际的 Origin，只包含协议、主机和端口，不
包含路径。`http_listen` 与 `web.listen` 不能使用同一个端口。

不要将未加密、未认证的 RPC 端口直接暴露到公网。远程公网访问应使用 SSH 隧道，
或者配置带 TLS 和身份认证的反向代理，并通过 `https://` 与 `wss://` 访问。

## 6. 准备 IB Gateway/TWS

启动 daemon 前确认：

1. IB Gateway 或 TWS 已登录；
2. API 连接已启用；
3. API 端口与配置一致；
4. `client_id` 没有被其他 API 客户端占用；
5. Paper 环境通常使用 Gateway 端口 `4002`，但应以 Gateway 实际设置为准；
6. Gateway 不应启用只读 API，否则无法提交订单。

如果暂时只想打开 Web 检查页面，可以将：

```toml
[ibkr]
connect_on_start = false
```

启动后再从“运行维护”页面连接 IBKR。

## 7. 前台启动

在项目根目录运行：

```bash
target/release/simple-quantitative-trader \
  --config config/local.toml \
  daemon
```

也可以在开发模式下启动：

```bash
cargo run -- --config config/local.toml daemon
```

日志出现 Web 和 RPC 监听成功后，打开：

```text
http://127.0.0.1:8080
```

如果 Web 显示无法连接 daemon，点击错误提示中的“配置 RPC 地址”，确认地址为：

```text
ws://127.0.0.1:8787
```

局域网访问时则使用服务器的局域网 IP。

## 8. 后台启动

仓库提供 GNU screen 启停脚本。首先安装 screen：

```bash
sudo apt install -y screen
```

确保 release 后端和 Web 都已经构建，然后运行：

```bash
deploy/screen-start.sh config/local.toml
```

查看会话：

```bash
screen -r quant-trader
```

退出查看但保持 daemon 运行：按 `Ctrl-A`，再按 `D`。

查看状态：

```bash
deploy/screen-status.sh config/local.toml
```

停止服务：

```bash
deploy/screen-stop.sh config/local.toml
```

日志保存在项目的 `logs/` 目录。

## 9. 更新后的重新构建与重启

修改 Web 后：

```bash
cd web
env -u NO_COLOR trunk build --release
cd ..
```

后端会在后续 HTTP 请求中读取新的 `web/dist` 文件，通常不需要因为纯 Web 修改而
重启 daemon。浏览器可能缓存旧的 WASM，必要时执行强制刷新。

修改后端后：

```bash
cargo test
cargo build --release
deploy/screen-stop.sh config/local.toml
deploy/screen-start.sh config/local.toml
```

数据库迁移会在 daemon 启动时自动执行。升级前建议先使用 Web“运行维护”页面或 CLI
创建备份。

## 10. 常见问题

### 页面返回 404 或空白

检查：

```bash
ls -la web/dist
```

并确认配置中的：

```toml
[web]
enabled = true
static_dir = "../web/dist"
```

### Web 能打开，但一直无法连接 RPC

确认：

- daemon 的 RPC 端口正在监听；
- Web“RPC 设置”中的地址使用 `ws://` 或 `wss://`；
- 从其他设备访问时没有错误使用 `127.0.0.1`；
- `allowed_web_origin` 与浏览器地址栏中的 Origin 完全一致；
- 防火墙允许访问相应端口。

### 浏览器报告 Origin 或连接被拒绝

本机默认值应为：

```toml
allowed_web_origin = "http://127.0.0.1:8080"
```

局域网访问时应换成实际服务器 IP。仅在受信任的隔离网络中临时诊断时才使用 `"*"`。

### 后端无法连接 IBKR

检查 Gateway/TWS 是否已登录、API 是否启用、端口是否正确，以及配置的
`client_id` 是否与其他客户端冲突。
