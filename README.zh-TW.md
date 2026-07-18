# Floret

<!-- readme-locales:start -->
<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a> |
  <strong>繁體中文</strong> |
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
  <strong>把 Agent 執行環境交給 Floret，產品始終掌握在你手中。</strong><br />
  為 Go 應用提供持久對話、工具執行、上下文生命週期和可觀測的執行期事實。
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Go 文件" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="授權條款" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go 版本" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

![Floret AI Agent 應用執行環境](okf/assets/readme/floret-agent-app-whiteboard.png)

## 為什麼選擇 Floret

讓模型呼叫工具只是 Agent 產品的開始。真正上線後，還需要可靠的模型迴圈、可持久化的對話紀錄、具備核准流程的工具執行、長上下文管理、可復原的工作，以及 UI 可以安心呈現的執行期事實。

Floret 提供這些底層能力，但不會接管你的產品。它不是 Agent UI、工作流程圖框架，也不是多 Agent 編排框架；它是支撐你所打造體驗的 Go 執行環境。

當產品必須保有自己的樣貌時，這個界線尤其重要。介面、身分系統、權限模型、模型路由、密鑰、資料模型與業務工具仍完全由你掌握；Floret 負責棘手的 Agent 執行生命週期。

### 為不願受制於預設的產品而生

- **保留模型接入權。** 可以使用內建設定，也可以實作 `runtime.ModelGateway`。Floret 管理請求與續接生命週期，傳輸與憑證仍歸產品所有。
- **定義真正屬於業務的 Agent。** 透過 `config.AgentProfile.SystemPrompt` 或 `config.Config.SystemPrompt` 定義角色、語氣、業務情境和操作規則，而不是交付千篇一律的通用助理。
- **讓工具和指令配合當下的業務狀態。** 以 `tools.Registry` 註冊嚴格的業務工具，再用 `runtime.ToolSurfaceProvider` 在執行的安全點更新工具、託管能力、指令和宿主上下文。
- **讓對話成為可靠資產。** `runtime.Host` 管理執行緒、回合、重試、分支、父層管理的子執行緒以及對 Provider 安全的歷史。
- **把核准策略留在產品裡。** Floret 理解通用副作用、資源與核准狀態；誰能做什麼、在何處做、為何允許，仍由你的產品決定。
- **讓執行過程清楚可見。** 將去識別事件、上下文壓力、壓縮事實及中立活動時間線接入任意 UI，無須暴露提示詞、密鑰或內部儲存紀錄。
- **測試產品，而非碰運氣。** Fake Provider 讓本機與 CI 的 Agent 流程維持可預測性。

## 產品始終由你掌握

Floret 的邊界刻意保持精簡：它負責引擎機制，產品決策永遠由應用程式掌控。

| Floret 負責 | 你的應用程式決定 |
| --- | --- |
| Provider 迴圈、重試、工具續接和結束狀態 | 使用者何時可開始、重試、中斷或取消工作 |
| 執行緒日誌、提示詞範圍、Provider 台帳和執行期產物 | 使用者、工作區、計費、產品中繼資料與保留策略 |
| 工具 Schema 驗證、通用副作用、核准生命週期與結果投影 | 授權、核准 UI、業務操作及面向使用者的文案 |
| 上下文壓力、壓縮生命週期及模型可見歷史 | 哪些產品資料可提供給模型，以及如何在介面中呈現 |
| 去識別事件及中立活動事實 | 版面、工作流程、控制項、路由與診斷策略 |

因此，維運主控台、程式開發環境、客服工具與產業助理都能共用可靠執行環境，卻不會變成同一種產品。

## 快速開始

安裝下游應用可用的穩定套件：

```bash
go get github.com/floegence/floret/config github.com/floegence/floret/runtime github.com/floegence/floret/tools github.com/floegence/floret/observation
```

```go
store := runtime.NewMemoryStore()
defer store.Close()
runtimeRoot, err := runtime.NewHostRuntime(store)
if err != nil { /* handle error */ }
threadCreator, err := runtime.NewThreadCreateHost(runtime.ThreadCapabilityOptions{Runtime: runtimeRoot})
if err != nil { /* handle error */ }

host, err := runtime.NewHost(runtime.HostOptions{
	Config: config.Config{
		Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "Hello from Floret.",
		AgentProfile: config.AgentProfile{ID: "support-agent", Name: "Support Agent"},
	},
	Runtime: runtimeRoot,
})
if err != nil { /* handle error */ }

thread, err := threadCreator.CreateThread(ctx, runtime.CreateThreadRequest{ThreadID: "thread-1"})
result, err := host.RunTurn(ctx, runtime.RunTurnRequest{
	ThreadID: thread.ID, TurnID: "turn-1", RunID: "run-1",
	Input: runtime.TurnInput{Text: "Welcome a new customer in one sentence."},
})
```

完整且可直接執行的範例見 [英文 README](README.md#quick-start)。若產品自行管理模型傳輸，請使用 OpenAI-compatible 設定或提供 `runtime.ModelGateway`。若要由 Floret 持久化其執行期資料，請使用 `runtime.OpenSQLiteStore(path)`；產品資料仍應放在自己的儲存空間，並以 `runtime.ThreadID` 關聯。

## 生產環境的接入方式

### 讓提示詞承載產品意圖

Floret 不會預設通用人設。使用 `config.AgentProfile.SystemPrompt` 或 `config.Config.SystemPrompt` 為 Agent 定義初始角色、語氣、業務情境和操作規則。提示詞是宿主的產品設定，因此客服專家、維運分析師、程式設計助理或產業專家可以共用同一個執行環境，不必改變執行環境本身。

若要依上下文調整行為，讓 `ToolSurfaceProvider` 回傳 `runtime.ToolSurface`。它可和工具表面、託管能力、宿主上下文一起替換目前系統提示詞，適合產品模式、工作區、權限或業務階段改變的情境。Floret 會在模型請求和本機工具分派前刷新這個表面，因此模型較早做出的決定不會悄然在新的指令或產品策略之外執行。

### 只授予執行環境真正需要的權限

使用 `tools.Registry` 定義業務動作。每個工具都有嚴格的 JSON Schema，也可描述副作用和資源。Floret 會驗證呼叫、在需要時執行核准流程、分派 Handler、記錄結果並產生模型可見結果；Handler 仍必須執行你的業務授權。

### 圍繞明確身分設計

- `ThreadID` 是持久對話的身分。
- `TurnID` 是一個面向使用者的回合。
- `RunID` 是一次具體的 Provider 執行。
- `PromptScopeID` 是提示詞快取與 Provider 台帳的重用邊界。

這些身分刻意彼此獨立。稍後結算宿主擁有的待處理工具工作時，必須使用已記錄的 Floret `ThreadID`、`TurnID` 與 `RunID`，而非 UI、稽核或展示 ID。

### 呈現事實，不要讀取引擎內部實作

設定 `runtime.EventSink` 來接收去識別的生命週期事件，並使用 `observation` DTO 呈現上下文壓力、壓縮和活動時間線。事件供宿主渲染與診斷使用，不是密鑰儲存，也不能取代產品資料庫。

若需要持久顯示投影，請使用 `ThreadTurnProjection` 與公開的 detail API。不要直接讀取 Floret 的儲存表，也不要在宿主重建 Provider 可見歷史。

### 讓測試維持可控

Fake Provider 可在沒有真實模型呼叫的情況下，測試工具行為、核准流程、重試、上下文壓力與宿主 UI 投影：

```bash
go test ./...
go run ./cmd/floret-test-ui
```

本機測試主控台供貢獻者檢查 Fake Provider 工作階段、去識別事件、工具情境和託管子執行緒使用；它不是下游整合介面。

## 參考資料

下游應用程式只應匯入以下公開套件：

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

`internal/` 下的內容皆為實作細節。API 以 [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) 為準；貢獻者可透過 [OKF 知識套件](okf/index.md) 了解執行環境架構和術語。

## 授權條款

Floret 採用 [MIT License](LICENSE)。
