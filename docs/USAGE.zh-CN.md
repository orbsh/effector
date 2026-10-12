# Effector —— 控制端接入指南

> **语言 / Languages:** [English](USAGE.md) (primary) · [中文](USAGE.zh-CN.md)

控制端要驱动一个 Effector 需要实现的全部内容：连接怎么建立、交换哪些帧、每一帧附带哪些义务。权威来源是 `crates/protocol` 里的帧类型与 `crates/runtime/src/remote.rs` 里的封装行为；可工作的控制端实现是 Aura 的 effector gateway（`aura/crates/engine/src/effectors.rs`），而 `crates/runtime/tests/` 下那些逐帧驱动的假控制平面就是验收测试。

## 1. 角色

Effector 是**客户端**：它主动拨出、注册，然后服务到达的帧。它不开端口，也不自发发起任何动作——它唯一会去取的 URL 是某个调用声明的 `code.url`。

Effector 同时是**技能盲**的：它看不到 skill、看不到操作名、看不到 LLM 提示词。到达的是 `session`（驻留身份）、`entry`（代码里的入口点）、`language`（用哪个运行时）、`args`、`code`（可执行部分）。把 skill 解析成这一组是控制端的工作——而且其中没有任何字段携带能力语义：`session` 与 `entry` 在 Effector 眼里只是一个身份与一个入口名，别无其他。

Effector 拥有运行时、常驻会话与 sandbox。控制端拥有身份、调度、deadline、ctx 作用域与策略（某个操作是 AI 现场生成还是预先审计过的）。

### 1.1 每一跳各跑什么协议

本文只描述**一跳**。Effector 夹在两跳之间，而两跳各有自己的 ABI：

- **外跳——控制端 ↔ Effector**：`crates/protocol` 里的帧，走 WS，一条消息一帧 JSON 文本（下文 §2–§8）。控制端自己面向客户端的协议在**控制端就终止了**——aura 的 realm 事件，以及 prism 的客户端事件协议（`signup` / `login` / `logout`、echo 事件、`turn.*`）。它们不跨这一跳：到达这里的是一个 `call`，从来不是一个 event。尤其：Effector 永远看不到账户、设备或登录——身份是控制端的载荷，只作为 `args` 的一部分到达脚本。
- **内跳——Effector ↔ 常驻扩展子进程**：**Induction**（`~/world/induction`，`crates/induction-{protocol,host,guest}`）——宿主指名一个可执行文件、spawn 它、握手、在 stdio 上投递类型化帧，并服务子进程回调上来的 `ctx.*` 各臂；子进程随宿主一起死。它在 effector 侧代码里只落在一处：进程载体 `crates/runtime/src/carrier/exec.rs`（加上持有子进程的会话注册表）。裸的 `exec` 一次性载体**不是** Induction——它按设计没有任何协议。
- 一个摊位骑哪一跳由控制端决定：`language` 指名载体，而对进程载体来说 spawn spec **就是**那个可执行文件。控制端若把载体嵌进自己的进程（aura 的 realm 就是这么做的），该摊位的外跳**消失**，内跳不变——这正是内跳挂在自己那套 ABI 上、而不挂在本文这套词汇上的原因。

所以是**两跳上的两个协议，不是同一个协议的两层**。它们由控制端首尾相连，任一侧都不需要知道另一侧的形状。

## 2. 连接与启动

Effector 启动顺序：

1. 读配置文件——`argv[1]`，默认 `effector.json`。
2. 从 `credential_env` 指名的环境变量读凭据。变量缺失是**启动失败**：进程不会拨号。
3. 拨 `control_plane_url` 并注册。此后断线即重连（§6）。

```json
{
  "control_plane_url": "ws://127.0.0.1:8787",
  "sandbox": true,
  "credential_env": "PROBE_CREDENTIAL",
  "capabilities": {
    "node_alias": "home-pc",
    "carriers": ["steel", "python", "induction"],
    "fs_scope": ["/home/user/work"],
    "command_exec": false,
    "network": "none"
  }
}
```

- `control_plane_url` —— 只认 WebSocket。按设计没有降级通道（long-polling 被否决过；WS-only 出网就是契约）。
- `sandbox` —— 默认 `true`，把会话进程包进 bubblewrap。远程部署必须保持开启（无人值守的外部代码）。
- `credential_env` —— 注册凭据是**用户**凭据，它从不写进配置文件。
- `capabilities.node_alias` —— **别名必须设置。** 默认值是空串，以 `""` 注册的 effector 无法按名字寻址（症状出现在控制端一侧："effector not connected"）。
- `capabilities.network` —— `"none"`、`{ "allow": ["host:port"] }` 或 `"open"`。

连接形态：一条 WS 消息恰好一个帧，JSON 文本，以 `"type"` 为内部 tag。Effector 的读端与写端是两个任务，所以连接是**全双工**的——一个调用执行期间帧仍可双向流动（§4.2 依赖这一点）。非文本 WS 消息被忽略。

## 3. 握手

Effector 发出的第一帧是 `register`；控制端必须回 `registered`。在这句应答之前，Effector 不服务任何东西。

```json
{ "type": "register", "node_alias": "home-pc", "credential": "tok-abc",
  "carriers": ["steel", "python", "induction"] }
```

```json
{ "type": "registered" }
```

- `node_alias` 是控制端一侧连接的键位。重连即重注册：控制端必须替换该别名此前的连接（旧的那条已经死了）。
- `credential` 在这里校验，并映射到用户命名空间。能力清单就是控制端写进该用户命名空间注册表的内容。

## 4. 帧

| 方向 | `type` | 用途 |
|---|---|---|
| effector → CP | `register` | 在册与能力握手（§3） |
| CP → effector | `registered` | 握手被接受 |
| CP → effector | `call` | 一个操作 |
| effector → CP | `result` | 该操作的应答 |
| effector → CP | `host`（`kind: "call"`） | 运行中的脚本发起的 `ctx.*` 宿主调用 |
| CP → effector | `host`（`kind: "result"`） | 该宿主调用的应答 |

### 4.1 `call` / `result`

`session` 是调用方给的不透明**驻留身份**：Effector 用它给常驻运行时定键（`effector/<node_alias>/<session>`），所以共享同一 `session` 的调用共享运行时状态（VM / module 全局），不同值之间绝不共享。它必须跨调用稳定——上游摊位 的 `type/key` 是自然取值。每次调用唯一的 `call_id` 顶不了这个位置：拿它定键等于每次都冷启一个运行时。

`entry` 是所交付代码里的入口点名字。它**不是**查表键：Effector 没有注册表，也不拿它的值解析任何东西。代码没有绑定这个名字时就是错误值，绝无魔法回落（每语言一种执行形态，分派表裁决：有运行时查表的语言用它——python/steel 的 dict/getattr、wasm 的导出表；没有的手写 `main` + match 字面名）。

`kind`/`stream` 承载统一信封的动词（ADR-0036）：每个派发调用都是流动词——`kind: "iterate_start"`（默认）带铸造的 `stream`，而 invoke 是首轮应答即终止的那个流。于是 `result` 的 `Ok` 载荷就是信封（plain handler 答 `{done: true, value: <返回>}`——裸返回由【载体】包成信封）。

`language` 必须是本节点真正携带的 carrier；未知或未编译进来的语言会体现在 `result` 的错误值里，而不是丢掉这次调用。

```json
{
  "type": "call",
  "call_id": "rp-7",
  "kind": "iterate_start",
  "stream": "stream-12",
  "session": "notes/7",
  "entry": "read_file",
  "language": "python",
  "args": { "path": "/tmp/x" },
  "code": { "url": "https://cdn.example/ab12…", "sha256": "ab12…" }
}
```

```json
{
  "type": "call",
  "call_id": "rp-8",
  "kind": "iterate_start",
  "stream": "stream-13",
  "session": "heavy/1",
  "entry": "main",
  "language": "induction",
  "args": {},
  "code": { "url": "https://cdn.example/3f9c…", "sha256": "3f9c…" }
}
```

（`wasmtime` 载体随 PLAN 4.21 退役——编译型语言骑 `induction` 原生子进程；同一种引用形态
用来投递内容寻址的二进制。）

`code` 只有一种形态：内容寻址的**引用**，字节从不进控制帧（ADR-0027——帧带引用，数据面
带字节）：

- `url` —— 由控制端用「部署声明的前缀 + 哈希」拼出来。服务端点只在它自己的 `/{hash}` 路径下
  暴露内容，所以这个 URL 在构造上就是自校验的。
- `sha256` —— **由本帧断言**，绝不从 URL 里解析：CDN 可能重写路径，而由传输细节推导出来的
  校验站在错的一侧。

收到调用后 Effector 解析该引用：按哈希命中缓存即直接用，否则 GET `url`（30 秒超时），在**执行
之前**用 `sha256` 校验字节，不匹配——或字节不是合法 UTF-8——一律是错误值，绝不静默接受。解析
出的源码按哈希缓存，活到进程结束（跨重连保留；进程重启从空开始）：内容寻址让驱逐变得平凡——
新代码就是新哈希、新键——所以缓存随互不相同的代码增长，而不随调用次数增长，里面也没有任何
需要恢复的东西。

```json
{ "type": "result", "call_id": "rp-7", "outcome": { "Ok": { "done": true, "value": { "ok": true } } } }
{ "type": "result", "call_id": "rp-7", "outcome": { "Err": "unknown language 'koto'" } }
```

每个 `call` 恰好得到一条 `result`，无论成功还是失败。成功载荷是统一信封（ADR-0036 §1）：plain handler 答 `{done: true, value}`——裸返回由载体包好；非终止起步的流在 Start 轮答 `{done: false, item, stream_id}`。结果按纪律保持在消息量级：大产物不走控制平面——由操作在 Effector 一侧自行处理。

### 4.2 `host` —— ctx 桥

运行中的脚本可以回调控制端。**脚本要持久化任何东西，走的就是这条路**：控制端的存储是持久事实，而 effector 在调用之间什么都不持有（它的常驻会话是可丢弃的热缓存）。状态外置是立项时的形状，不是权宜通道——effector 不持有存储、也不持有自己的数据面凭证，正是因此。

脚本函数到线上 op 的对应关系：

| 脚本函数 | `op.op` | 载荷 |
|---|---|---|
| `ctx.invoke` | `invoke` | `target_type`、`target_key`、`handler`、`args` |
| `ctx.iterate.start` | `iterate` | `target_type`、`target_key`、`handler`、`args` |
| `ctx.iterate.next` | `iterate_next` | `stream_id` |
| `ctx.iterate.dispose` | `iterate_dispose` | `stream_id` |
| `ctx.store` | `store_emit` | `instruction` —— 一条 okm Collection 指令，作为数据承载（脚本写出的 `{"collection": …, "op": …, …}` 原样；控制端按该类型自己的存储计划执行） |
| `ctx.emit` | `emit` | `event`、`data` —— 控制端在自己的 realm 解析路由；业务面 fire-and-forget，帧层仍是请求/应答（成功应答为 Null） |

**远程面范围（ADR-0043 同步注记）**：线上承载上表各臂——`timer.register/cancel`、
`queue.depth/skip_to_head`、`schema` 目前是进程内 carrier 的成员，尚无远程臂。远程摊位
调用它们得到的是未绑定名（steel：加载失败；python：AttributeError）——不是静默 no-op。
它们以 `emit` 同样的方式上线：协议 enum 臂 + 控制端 resolver + effector 绑定，一波完成。

```json
{
  "type": "host", "kind": "call", "host_call_id": "h-1", "call_id": "rp-7",
  "op": { "op": "store_emit", "instruction": {
      "collection": "counters", "doc": {"visits": 1}, "key": {"id": 1},
      "op": "put_document" } }
}
```

```json
{
  "type": "host", "kind": "result", "host_call_id": "h-1",
  "outcome": { "Ok": { "ok": true } }
}
```

- `call_id` 是**外层** `call` 的 id —— 控制端就是靠它解析 ctx 作用域（正在读写哪个摊位实例的字段）。Effector 不需要知道自己的实例键。
- `host_call_id` 由 Effector 铸造，每次宿主调用唯一。应答必须回显它；id 对不上的应答会被静默丢弃。
- **脚本的宿主调用会一直阻塞到应答到达。** 这一侧没有 deadline：没人应答的 `host` 调用会卡住这次脚本调用，直到连接断开。每个 `host` 调用都要答。
- 宿主调用会**在**外层 `call` 仍在执行时到达 —— "读一个 call、答一个、再读下一个"的半双工服务器承载不了这个。
- `ctx.store` 是走线的持久化通道：effector 从不解析那条指令（schema 随类型注册住在控制端）；未声明存储的类型会收到错误字符串应答。

## 5. 义务清单

1. 先以 `registered` 应答 `register`，再发别的。
2. 只发上表里的帧，且字段齐全。Effector 的读端遇到不认识的帧、或解析不过去的帧（字段缺失/类型不对）会**拆掉连接**然后重连——两者都当作协议错误，不要当作前向兼容。
3. 每个请求铸造唯一的 `call_id`，自己维护关联。
4. 每个 `host` 调用都要答——脚本阻塞在上面。
5. 调用 deadline 归控制端。Effector 对调用没有 deadline；结果只在活着的连接上送达，所以进行中的调用随连接一起死，控制端的超时是唯一能结束它的东西。
6. 把重连当作重注册：替换该别名的连接，并预期要重新驱动 Effector 不持有的那些状态（它在调用之间什么都不持有）。

## 6. 重连与生命周期

- 任何一次断开后，Effector 以指数退避重连：1s、2s、4s …… 封顶 60s。**退避从不重置**——即使已健康运行一小时，此后断线仍按当前（最高 60s）的延迟重连。
- 控制端**干净**关闭连接不会触发重连：Effector 视其为致命（`control plane closed the connection cleanly`）并退出进程。只有当你确实想让那台机器上的这个 effector 消失时才这么用。
- 常驻会话状态是进程内存；它跨重连存活，但不跨驱逐或进程退出。持久事实住在控制端的存储里。

## 7. 报文转录

一次完整会话，一帧一行，如实出现在线上（上文是格式化版本，这里是串联后的流程）：

```
effector → CP   {"type":"register","node_alias":"home-pc","credential":"tok-abc","carriers":["steel","python"]}
CP → effector   {"type":"registered"}
CP → effector   {"type":"call","call_id":"rp-7","kind":"iterate_start","stream":"stream-9","session":"counter/k1","entry":"counter","language":"steel","args":{"n":4},"code":{"url":"http://cp.local/code/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}}
effector → CP   {"type":"host","kind":"call","host_call_id":"h-1","call_id":"rp-7","op":{"op":"store_emit","instruction":{"collection":"counters","doc":{"visits":1},"key":{"id":1},"op":"put_document"}}}
CP → effector   {"type":"host","kind":"result","host_call_id":"h-1","outcome":{"Ok":{"ok":true}}}
effector → CP   {"type":"result","call_id":"rp-7","outcome":{"Ok":{"done":true,"value":{"n":4}}}}
```

（`code` 是引用形态——这里的哈希是占位值：effector 去取那个 URL、把断言里的 sha256 与字节
核对，然后运行解析出来的 steel 源码 `(define (counter args) (hash "n" 4))`。这段拼接是示意
性的——"先写一条存储指令再返回"只是调用可以长的众多形状之一。）

## 8. 失败语义

| 输入 | Effector 的行为 |
|---|---|
| 未知 `language` / carrier 未编译进来 | 以 `Err` 结果的 `result` 应答 |
| `code.url` hash 不匹配、取件超时（30s）、代码非 UTF-8 | 以 `Err` 结果的 `result` 应答 |
| 控制端发来不认识的帧类型，或解析不过去的帧 | 拆掉连接，随后重连 |
| 非文本 WS 消息 | 忽略 |
| 启动时 `credential_env` 变量缺失 | 进程在拨号前退出 |
| `host` 结果的 `host_call_id` 不认识 | 丢弃（调用方已放弃） |

## 9. 演进

新的帧类型与字段以新增小节的方式追加到本文档；改变某个既有帧的含义属于 `docs/PLAN.md` 里落地它的那个 phase，以及 protocol crate 的文档注释——那是这份契约可被机器检查的那一半。