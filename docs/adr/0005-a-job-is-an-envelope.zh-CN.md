# 0005 — Job 是信封，不是第二个 Plan

> **语言：** [English](0005-a-job-is-an-envelope.md)（主） · [中文](0005-a-job-is-an-envelope.zh-CN.md)

**状态：** 提议（2026-11-19）。交付任务的第一步：类型与词汇。本记录之下不改变任何行为。
**日期：** 2026-11-19

## 背景

摊位只能回答它被给予的那一轮，别的做不了。它无法发起工作：没有任何方式让它说"把这些步骤跑一下"，于是摊位需要执行的一切，要么被编译进宿主（`Body::Rust`），要么被预先摆成分发层的固定节点。gravity 的 skill 执行今天就住在那儿，而那就是本记录要处理的复杂性。

"怎么跑"这项知识也待在错的层里。`Body::Script { language, source, encoding }` 是一份 spawn spec，它住在 aura 的分发类型里，也是"gravity 部署在哪"这类问题至今只能靠改分发层回答的原因。

同时，平台词汇里已经有三样不能混淆的东西：

| 词 | 是什么 | 在哪 |
|---|---|---|
| `Plan` | 定义：步骤、类型、边的图 | `schematic`（plan 是图） |
| `Run` | 一次执行的那条日志，只追加 | `schematic`（run 是 append log） |
| turn | 摊位的一次请求与回答 | aura / gravity |

本记录要的那个东西需要第四个词，而它不是"plan"：plan 已经是定义的意思，而摊位把**同一份** plan 提交两次，跟把 plan 定义一次是两个不同的行为。

## 决定

**裁决 —— 摊位交给平台的单位是 JOB。Job 是一个信封：它按引用指明跑什么（而不是重新定义），指明它能碰什么，以及结果算谁的状态。**

```rust
struct Job {
    id: JobId,              // 幂等键：重放的提交只跑一次
    booth: InstanceKey,     // 结果算谁的状态
    scope: Scope,           // Turn | Shared
    source: JobSource,      // 跑什么
    resources: Vec<ResourceReq>,
    placement: Placement,   // Any | Labels([...])
}

enum JobSource {
    Ops(Vec<Op>),                            // 几次调用，没有图
    Plan { plan_hash: String, run: RunId },  // schematic 的定义 + 要推进的那次 run
}

struct Op {
    call: ToolCall,         // executor / entry / language / code：既有的调用形状
    needs: Need,            // 这个 op 得到的 ctx 能力
    on_error: OnError,      // Fail | Skip | Retry(n) | Never
}

struct ResourceReq {
    name: String,           // op 如何引用它
    kind: ResourceKind,     // Git { url, rev } | S3 { bucket, prefix } | Store { ns }
    mode: Mode,             // Read | Write
}
```

1. **Job 引用 plan，从不复述它。** `JobSource::Plan` 带 `plan_hash` 与要推进的 run，于是 schematic 始终是"plan 是什么"的唯一权威，`Step` 保持原义（图上的一个节点），而 `Op` 是 Job 自己的东西：一次带 ctx 与失败策略的调用。带 `Ops` 的 Job 只是"图退化成一条线"的那种 Job。

2. **Job 不决定在哪儿跑、跑多久。** 选点是 realm 的事（按声明的能力、标签与就近）；生命周期是节点的 `LeaseConfig`（`idle_secs`、`max_idle_secs`、`exit_on_disconnect`）；编码按腿协商。Job 只声明 `scope`（Turn 或 Shared），因为 scope 是工作本身的事实，而 `Turn` 正是"一次性执行"可以被表达而不必特判的原因。

2a. **信封里没有用户。** `Scope` 没有 `User` 变体，也没有任何字段指称用户。aura 没有这个概念，一个携带它的 Job 会逼着 aura 长出这个概念。工作是**给谁的**属于摊位业务：它走 op 的 `args`，由摊位自己的代码读取，平台永远不知道。同一个理由让用户身份也不能作为选点依据 —— 见 2b。

2b. **选点是标签，标签是控制面的词。** `Placement::Labels(["u101:laptop"])` 要求一个带该标签的节点；节点自己不能声明它。Effector 的 `Register` 帧说节点**是什么**（别名、载体、容器类），不说它是**谁** —— 身份随拨入的 token 到达，由控制面校验。所以标签由 prism 从授权生成并挂到连接上；一个能给自己起标签的节点，也能给别人起。

3. **资源是声明的，不是顺手创建的。** Job 说"这个 rev 的仓库，只读"；realm 授权，节点物化。op 碰到的任何不在 `resources` 里的东西，是 Job 的缺陷，不是执行器的能力。

4. **`OnError::Never` 有其存在理由。** 重试策略属于 Job，不属于执行器的直觉；有副作用的步骤（推送、写入）必须能说"别跑第二遍"。默认失败即重试，恰好瞄准了最要紧的那些步骤 —— 那是陷阱。

5. **Job 的帧说明自己属于哪个 Job、哪个 op。** 摊位本来就在为它自己那一轮发帧；Job 回来的帧是同一条通道上的第二条流，没有标识的话，摊位分不清自己看到的是什么。

6. **附件承载 job 需要、但本身不表达的东西。** `Job::attachments` 是一串 `{name, media, bytes}`：一个通用槽位，对本 crate 不透明。它存在的理由很具体 —— 一个要跑"提交时才定义的类型"的 job（从库里取出的 SKILL、图里的节点）无法只在 `ops` 里说清它们，而远程提交也没有别的地方放这些定义。它之所以是附件而不是 `JobSource` 的一个变体，是因为变体会让信封学会填它那一方的词汇（body、storage schema）；附件只让信封学会"有东西随这个 job 旅行"，含义留在填它的那一侧。

   三条规则防止这个槽位变成跨越边界（那道边界本该只有帧能穿过）的无类型侧信道：由**派单者**填写 —— 摊位不能把不属于它这次提交的定义塞给 realm；**接收方**封顶总额，超出即拒；不认识的 `name` 或 `media` 一律**拒绝**，绝不静默跳过 —— 没到达的定义不能让 job 看起来健康。

## 后果

- **realm 持有 job 行。** Job 先是数据，然后才是工作：可查、可重试、可重放 —— 把 spawn spec 塞在分发层里的做法从来给不了这些。行是 `(id, attempt, state)` 加 Job 文档，与 k10r 已有的 run 表同形。
- **摊位只多一个动词。** `ctx.plan(job)`（动词名仍待定）提交并返回一个流句柄；结果走 induction-guest 已有的 iterate 面（`IterateStart`/`Next`/`Dispose`），不引入新的流概念。
- **`Body::Script` 的 spawn spec 要搬出分发层。** 本记录不执行这次搬迁；它只是指出 `language`、`entry`、`code` 属于 Job，于是搬迁有了目的地。
- **schematic 是载体，不是这个类型的依赖。** `JobSource::Plan` 用 hash 与 run id 指认，至于节点是否用规划中的 `plan` 载体臂去跑，是节点的事。
- **编译进宿主的 `Body::Rust` 变成一个带代价的选择。** 以 Job 形态发布的 body 可以不重编译平台就改变；编译进去的不能。本记录不禁止编译形态 —— 只是把它从"唯一办法"降为"例外"。

## 允许嵌套，而且这正是它的意义

Job 可以再提交 Job：gravity 执行那种"步骤本身就是摊位调用"的 skill 时，靠的就是这一条，而不必自己内嵌一层分发。三条限制让它成为一棵树而不是一场风暴：

- **深度有上限**（数字是 realm 的策略，不是信封的）。超限的嵌套 Job 作为错误值被拒，不静默丢弃。
- **子 Job 的 scope 不得大于父的。** `Turn` 不能生出 `Shared`：scope 向下收窄，于是被丢弃的父不会留下一个持久的子。
- **每一帧都带自己的路径。** `(job, op)` 本就是 Job 结果的必需项；嵌套 Job 的帧带整条链，于是摊位能分清"我的第 3 步说的"与"孙辈说的"。

## 动词

这个动作是 DISPATCH（派单）：摊位派发一个 job。名词是 job，因为在这个系统里 "task" 已经指用户的工作项（一个带 sheet 的顶点，k10r ADR-0021），给这个词加第二个意思是本套词汇存在的意义所在 —— 要避免的正是它。派单运送的是 job；job 不是 task。

仍待定：ctx 面上的确切拼写（`ctx.dispatch(job)`），以及摊位能否在"一轮之外"派单。

## 相关记录

effector ADR-0002（摊位是一个 body）、ADR-0003（环境模型 —— 租约与容器形态）、ADR-0004（载体是一个接口）、gravity ADR-0011（组合是图 —— schematic 的 `Plan`/`Run`）、gravity ADR-0013（编码是传输轴）。