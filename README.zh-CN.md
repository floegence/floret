# Floret

<!-- readme-locales:start -->
<p align="center">
  <a href="README.md">English</a> |
  <strong>简体中文</strong> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <a href="README.ja-JP.md">日本語</a> |
  <a href="README.ko-KR.md">한국어</a> |
  <a href="README.de-DE.md">Deutsch</a> |
  <a href="README.fr-FR.md">Français</a> |
  <a href="README.es-ES.md">Español</a> |
  <a href="README.pt-BR.md">Português do Brasil</a> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>把 Agent 运行时交给 Floret，把产品始终握在自己手中。</strong><br />
  为 Go 应用提供持久对话、工具执行、上下文生命周期和可观测的运行时事实。
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Go 文档" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="许可证" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go 版本" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

<p align="center">
  <a href="#为什么选择-floret">为什么选择 Floret</a> ·
  <a href="#产品始终由你掌控">产品始终由你掌控</a> ·
  <a href="#快速开始">快速开始</a> ·
  <a href="#生产环境的接入方式">生产环境接入</a> ·
  <a href="#参考资料">参考资料</a>
</p>

![Floret AI Agent 应用运行时](okf/assets/readme/floret-agent-app-whiteboard.png)

## 为什么选择 Floret

让模型调用工具只是 Agent 产品的开始。真正上线后，还需要可靠的模型循环、可持久化的会话记录、带审批的工具执行、长上下文管理、可恢复的工作，以及 UI 可以放心呈现的运行时事实。

Floret 提供这些底层能力，却不会接管你的产品。它不是 Agent UI、工作流图框架，也不是多 Agent 编排框架；它是支撑你正在打造的体验的 Go 运行时层。

当产品必须有自己的样子时，这条边界尤为重要。界面、身份体系、权限模型、模型路由、密钥、数据模型和业务工具仍完全由你控制；Floret 负责棘手的 Agent 执行生命周期。

### 为不接受预设的产品而生

- **保留模型接入权。** 可使用内置配置，也可实现 `runtime.ModelGateway`。Floret 管理请求与续接生命周期，传输和凭据仍归产品所有。
- **定义真正属于业务的 Agent。** 通过 `config.AgentProfile.SystemPrompt` 或 `config.Config.SystemPrompt` 定义角色、语气、业务场景和操作规则，而不是交付一个千篇一律的通用助手。
- **让工具和指令随业务状态变化。** 通过 `tools.Registry` 注册严格的业务工具；再用 `runtime.ToolSurfaceProvider` 在一次运行的安全点更新工具、托管能力、指令和宿主上下文。
- **让对话成为可靠资产。** Floret runtime 管理线程、回合、重试、分叉、父级管理的子线程以及对 Provider 安全的历史。
- **把审批策略留在产品中。** Floret 理解通用副作用、资源与审批状态；谁能做什么、在何处做、为何允许，仍由你的产品决定。
- **让运行过程可见。** 将脱敏事件、上下文压力、压缩事实和中立活动时间线接入任意 UI，无需暴露提示词、密钥或内部存储记录。
- **测试产品，而不是碰运气。** Fake Provider 让本地和 CI 中的 Agent 流程保持确定性。

## 产品始终由你掌控

Floret 的边界刻意保持紧凑：它负责引擎机制，产品决策始终由应用掌握。

| Floret 负责运行 | 你的应用决定 |
| --- | --- |
| Provider 循环、重试、工具续接和结束状态 | 用户何时可以发起、重试、中断或取消工作 |
| 线程日志、提示词作用域、Provider 台账和运行时产物 | 用户、工作区、计费、产品元数据和保留策略 |
| 工具 Schema 校验、通用副作用、审批生命周期和结果投影 | 鉴权、审批 UI、业务操作和面向用户的文案 |
| 上下文压力、压缩生命周期与模型可见历史 | 哪些产品数据可以提供给模型，以及如何在界面中呈现 |
| 脱敏事件和中立活动事实 | 布局、工作流、控件、路由和诊断策略 |

因此，运维控制台、编程环境、客服工具和行业助手可以共用可靠运行时，却不必被塑造成同一种产品。

## 快速开始

安装面向下游应用的稳定包：

```bash
go get github.com/floegence/floret/config github.com/floegence/floret/runtime github.com/floegence/floret/tools github.com/floegence/floret/observation
```

```go
store := runtime.NewMemoryStore()
defer store.Close()
var createBinder *runtime.ThreadCreateHostBinder
var turnBinder *runtime.TurnExecutionHostBinder
err := runtime.ConfigureHostCapabilities(store, func(bootstrap *runtime.HostBootstrap) error {
	var err error
	createBinder, err = runtime.NewThreadCreateHostBinder(bootstrap)
	if err != nil { return err }
	turnBinder, err = runtime.NewTurnExecutionHostBinder(bootstrap)
	return err
})
if err != nil { /* handle error */ }

createIntentID := runtime.CreateIntentID("create-thread-1")
threadCreator, err := createBinder.Bind("thread-1", createIntentID)
if err != nil { /* handle error */ }
turnFactory, err := turnBinder.Bind("thread-1")
if err != nil { /* handle error */ }
thread, err := threadCreator.CreateThread(ctx, runtime.CreateThreadRequest{
	ThreadID: "thread-1", CreateIntentID: createIntentID,
})
if err != nil { /* handle error */ }
turnHost, err := turnFactory.NewHost(ctx, runtime.TurnExecutionHostOptions{
	Config: config.Config{
		Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "Hello from Floret.",
		AgentProfile: config.AgentProfile{ID: "support-agent", Name: "Support Agent"},
	},
})
if err != nil { /* handle error */ }

result, err := turnHost.RunTurn(ctx, runtime.RunTurnRequest{
	ThreadID: thread.ID, TurnID: "turn-1", RunID: "run-1",
	Input: runtime.TurnInput{Text: "Welcome a new customer in one sentence."},
})
```

完整、可直接运行的示例请见 [英文 README](README.md#quick-start)。产品自行管理模型传输时，使用 OpenAI-compatible 配置或提供 `runtime.ModelGateway`。需要由 Floret 持久化其运行时数据时，使用 `runtime.OpenSQLiteStore(path)`；你的产品数据仍应保存在自己的存储中，并以 `runtime.ThreadID` 关联。

## 生产环境的接入方式

### 让提示词承载产品意图

Floret 不会预设通用人设。使用 `config.AgentProfile.SystemPrompt` 或 `config.Config.SystemPrompt` 为 Agent 定义初始角色、语气、业务场景和操作规则。提示词属于宿主的产品配置，因此客服专家、运维分析师、编程助手或行业专家可以共享同一运行时，而不必改变运行时本身。

需要根据上下文调整行为时，让 `ToolSurfaceProvider` 返回 `runtime.ToolSurface`。它可以和工具表面、托管能力、宿主上下文一起替换当前系统提示词，适合产品模式、工作区、权限或业务阶段发生变化的场景。Floret 会在模型请求和本地工具分发前刷新该表面，因此模型较早做出的决定不会悄然运行在新的指令或产品策略之外。

### 只授予运行时必需的权限

使用 `tools.Registry` 定义业务动作。每个工具都有严格的 JSON Schema，并可描述副作用和资源。Floret 会校验调用、在需要时走审批流程、分发 Handler、记录结果并生成模型可见的结果；Handler 仍必须执行你的业务鉴权。

### 围绕明确的身份设计

- `ThreadID` 是持久对话的身份。
- `TurnID` 是一个面向用户的回合。
- `RunID` 是一次具体的 Provider 执行。
- `PromptScopeID` 是提示词缓存和 Provider 台账的复用边界。

这些身份有意彼此独立。将来结算宿主拥有的待处理工具工作时，必须使用记录下来的 Floret `ThreadID`、`TurnID` 与 `RunID`，而不能使用 UI、审计或展示 ID。

### 呈现事实，而不是引擎内部实现

设置 `runtime.EventSink` 以接收脱敏的生命周期事件，并使用 `observation` DTO 呈现上下文压力、压缩和活动时间线。事件用于宿主渲染和诊断，不是密钥存储，也不能代替产品数据库。

需要持久的显示投影时，请使用 `ThreadTurnProjection` 和公开的 detail API。不要直接读取 Floret 的存储表，也不要在宿主中重建 Provider 可见历史。

### 让测试可控

Fake Provider 使工具行为、审批流程、重试、上下文压力和宿主 UI 投影都能在没有真实模型调用的情况下测试：

```bash
go test ./...
go run ./cmd/floret-test-ui
```

本地测试控制台面向贡献者，用于检查 Fake Provider 会话、脱敏事件、工具场景和托管子线程；它不是下游集成接口。

## 下游集成接口

下游应用只应导入以下公开包：

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

`internal/` 下的内容均为实现细节。API 以 [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) 为准；贡献者可通过 [OKF 知识包](okf/index.md) 了解运行时架构和术语。

## 许可证

Floret 使用 [MIT License](LICENSE)。
