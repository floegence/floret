# Floret v5

Floret 是面向 Go 应用的可复用交互式 AI Agent 引擎。它负责模型循环、持久化会话、工具执行、审批、上下文、SubAgent、恢复、provider state 与可观测运行事实；宿主继续负责产品 UI、路由、凭据、授权策略和产品数据。

模块路径固定为 `github.com/floegence/floret/v5`。完整且权威的 v5 API、快速开始、存储 SPI 与迁移说明见 [README.md](README.md)。

v5 的关键边界：

- 普通应用只使用 `identity`、`config`、`runtime`、`observation`、`tools`、官方 `provider` 构造器和 opaque `storage.Source`；高级存储实现使用独立的 `storage/spi`。
- 所有模型请求只经过显式 `provider.Gateway`，不存在内部 provider fallback 或生产 fake response。
- `runtime.Host` 只保存在 composition root；宿主通过一个 typed `ThreadService` 读写 Thread、turn、审批、队列与 SubAgent 生命周期。
- 宿主只提交稳定的 `identity.LogicalRequestID`，Thread、turn、run 与子线程身份都由 Floret 分配；相同请求永久 replay，输入冲突返回 typed error。
- Floret 的 journal、turn projection、approval、Todo、SubAgent、artifact、provider state 和 prompt cache 是 Agent 生命周期的唯一事实来源。
- 每个线程使用单调 `view_version`；订阅仅提供实时观察，断线后重新读取权威 `View`。
- `runtime.Open` 自动迁移 Floret 自有且受支持的 domain schema。外部 v2.2 物理存储仍需使用显式转换流程。
- 生产集成禁止 `replace`、`go.work` 和 sibling repository path。

```bash
go get github.com/floegence/floret/v5@v5.0.0
GOWORK=off go test ./...
```
