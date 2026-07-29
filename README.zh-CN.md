# Floret v2

Floret 是面向 Go 应用的可复用交互式 AI Agent 引擎。它负责模型循环、持久化会话、工具执行、审批、上下文、SubAgent、恢复、provider state 与可观测运行事实；宿主继续负责产品 UI、路由、凭据、授权策略和产品数据。

模块路径固定为 `github.com/floegence/floret/v2`。完整且权威的 v2 API、快速开始、存储 SPI 与迁移说明见 [README.md](README.md)。

v2 的关键边界：

- 公共包仅为 `config`、`provider`、`runtime`、`storage`、`tools`、`observation`，以及仅供测试使用的 `florettest`。
- 所有模型请求只经过显式 `provider.Gateway`，不存在内部 provider fallback 或生产 fake response。
- `runtime.Host` 只保存在 composition root，并按身份签发 `ThreadCreator`、`ThreadReader`、`TurnRunner`、`SubAgentManager` 和 recovery 等窄句柄。
- `runtime.Agent` 在构造时复制配置和静态工具，之后不可变。
- Floret 的 journal、turn projection、approval、Todo、SubAgent、artifact、provider state 和 prompt cache 是 Agent 生命周期的唯一事实来源。
- v2 启动不自动迁移。精确 schema-v16 必须先显式执行 `floret-store migrate-v2`；其他旧版、未知或损坏 schema 直接拒绝。
- 生产集成禁止 `replace`、`go.work` 和 sibling repository path。

```bash
go get github.com/floegence/floret/v2@v2.0.0
GOWORK=off go test ./...
```
