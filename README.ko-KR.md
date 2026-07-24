# Floret

<!-- readme-locales:start -->
<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <a href="README.ja-JP.md">日本語</a> |
  <strong>한국어</strong> |
  <a href="README.de-DE.md">Deutsch</a> |
  <a href="README.fr-FR.md">Français</a> |
  <a href="README.es-ES.md">Español</a> |
  <a href="README.pt-BR.md">Português do Brasil</a> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>에이전트 런타임은 Floret에 맡기고, 제품의 주도권은 직접 유지하세요.</strong><br />
  Go 애플리케이션을 위한 영속 대화, 도구 실행, 컨텍스트 수명 주기, 관측 가능한 런타임 정보.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Go 레퍼런스" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="라이선스" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go 버전" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

![Floret AI 에이전트 앱 런타임](okf/assets/readme/floret-agent-app-whiteboard.png)

## 왜 Floret인가요?

모델이 도구를 호출하게 하는 일은 에이전트 제품의 시작일 뿐입니다. 실제 서비스에는 견고한 모델 루프, 영속적인 대화 기록, 승인 절차를 거치는 도구 실행, 긴 컨텍스트 관리, 복구 가능한 작업, 그리고 UI가 신뢰하고 보여 줄 수 있는 런타임 정보가 필요합니다.

Floret은 이 실행 기반을 제공하지만 제품을 대신 정의하지는 않습니다. 에이전트 UI도, 워크플로 그래프도, 멀티 에이전트 오케스트레이션 프레임워크도 아닙니다. 여러분이 만드는 경험 뒤에서 동작하는 Go 런타임입니다.

제품이 진정으로 고유해야 한다면 이 경계는 중요합니다. UI, ID 체계, 권한 모델, 모델 라우팅, 시크릿, 데이터 모델, 도메인 도구는 계속 제품이 소유합니다. Floret은 어려운 에이전트 실행 수명 주기를 맡습니다.

### 정해진 틀에 맞출 수 없는 제품을 위해

- **모델 경로는 직접 통제합니다.** 내장 설정을 쓰거나 `runtime.ModelGateway`를 구현할 수 있습니다. Floret은 요청과 연속 실행 수명 주기를 관리하고, 전송과 자격 증명은 제품이 관리합니다.
- **업무에 맞는 고유한 역할을 Agent에 부여합니다.** `config.AgentProfile.SystemPrompt` 또는 `config.Config.SystemPrompt`로 역할, 어조, 업무 시나리오, 운영 규칙을 정의할 수 있습니다. 모두 같은 범용 어시스턴트를 출시할 필요가 없습니다.
- **업무 상황에 맞춰 도구와 지시문을 바꿉니다.** `tools.Registry`에 엄격한 도메인 도구를 등록하고, `runtime.ToolSurfaceProvider`로 실행 중 안전한 시점에 도구, 호스팅 기능, 지시문, 호스트 컨텍스트를 새로 고칠 수 있습니다.
- **대화를 신뢰할 수 있는 자산으로 만듭니다.** Floret 런타임은 스레드, 턴, 재시도, 포크, 부모가 관리하는 하위 스레드, Provider에 안전한 기록을 관리합니다.
- **승인 정책은 제품에 둡니다.** Floret은 일반적인 효과, 리소스, 승인 상태를 다룹니다. 누가 무엇을 어디서 왜 할 수 있는지는 제품이 결정합니다.
- **실행 상태를 드러냅니다.** 프롬프트, 시크릿, 내부 저장소 레코드를 노출하지 않고 정제된 이벤트, 컨텍스트 압력, 압축 정보, 중립적인 활동 타임라인을 어떤 UI에나 연결합니다.
- **모델 운에 기대지 않고 테스트합니다.** Fake Provider로 로컬과 CI에서 에이전트 흐름을 결정적으로 검증할 수 있습니다.

## 제품의 주도권을 지키는 경계

Floret의 경계는 의도적으로 좁습니다. 엔진의 동작 원리는 Floret이, 제품의 판단은 애플리케이션이 맡습니다.

| Floret이 실행하는 일 | 애플리케이션이 결정하는 일 |
| --- | --- |
| Provider 루프, 재시도, 도구 연속 실행, 종료 상태 | 사용자가 작업을 시작, 재시도, 중단, 취소할 수 있는 시점 |
| 스레드 저널, 프롬프트 범위, Provider 원장, 런타임 아티팩트 | 사용자, 워크스페이스, 과금, 제품 메타데이터, 보존 정책 |
| 도구 스키마 검증, 일반 효과, 승인 수명 주기, 결과 투영 | 권한 부여, 승인 UI, 도메인 작업, 사용자용 문구 |
| 컨텍스트 압력, 압축 수명 주기, 모델에 보이는 기록 | 모델에 전달할 수 있는 제품 데이터와 UI 표시 방식 |
| 정제된 이벤트와 중립 활동 정보 | 레이아웃, 워크플로, 제어 요소, 라우팅, 진단 정책 |

이 경계 덕분에 운영 콘솔, 코딩 환경, 고객 지원 도구, 산업별 어시스턴트는 하나의 신뢰할 수 있는 런타임을 공유하면서도 서로 같은 제품이 될 필요가 없습니다.

## 빠른 시작

다운스트림 애플리케이션용 안정 패키지를 설치합니다.

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

그대로 실행할 수 있는 전체 예제는 [영문 README](README.md#quick-start)에 있습니다. 모델 전송을 제품이 직접 소유한다면 OpenAI-compatible 설정을 사용하거나 `runtime.ModelGateway`를 제공합니다. Floret이 자체 런타임 데이터를 영속화해야 한다면 먼저 `runtime.InspectSQLiteStore`를 실행합니다. `missing` 또는 `empty` 상태의 저장소는 해당 초기화 전용 상태로 바로 엽니다. `current` 상태의 저장소는 `runtime.VerifySQLiteStore`로 검증합니다. 업그레이드 가능한 저장소는 `runtime.MigrateSQLiteStore`를 `apply` 모드로 명시적으로 실행한 뒤 다시 검증합니다. `current` 또는 마이그레이션된 저장소에서는 최종 검증 결과의 상태와 관찰된 스키마 ID를 `runtime.OpenSQLiteStore(ctx, path, request)`에 전달합니다. `OpenSQLiteStore`는 스키마를 암시적으로 마이그레이션하지 않습니다. 제품 데이터는 자체 저장소에 두고 `runtime.ThreadID`로 연결하세요.

## 프로덕션 연결 방식

### 프롬프트에 제품 의도를 담으세요

Floret은 범용 페르소나를 강요하지 않습니다. `config.AgentProfile.SystemPrompt` 또는 `config.Config.SystemPrompt`로 Agent의 초기 역할, 어조, 업무 시나리오, 운영 규칙을 정의하세요. 프롬프트는 호스트가 소유하는 제품 설정이므로 고객 지원 전문가, 운영 분석가, 코딩 어시스턴트, 산업별 전문가가 런타임 자체를 바꾸지 않고 같은 기반을 공유할 수 있습니다.

상황에 따라 동작을 바꿔야 한다면 `ToolSurfaceProvider`에서 `runtime.ToolSurface`를 반환하세요. 도구 표면, 호스팅 기능, 호스트 컨텍스트와 함께 현재 시스템 프롬프트를 교체할 수 있습니다. 제품 모드, 워크스페이스, 권한, 업무 단계의 변경에 알맞습니다. Floret은 모델 요청과 로컬 디스패치 전에 표면을 새로 고치므로, 오래된 모델 판단이 새 지시문이나 제품 정책 밖에서 조용히 실행될 수 없습니다.

### 런타임에는 꼭 필요한 권한만 부여하세요

도메인 동작은 `tools.Registry`로 정의합니다. 각 도구에는 엄격한 JSON Schema가 있고 효과와 리소스를 설명할 수 있습니다. Floret은 호출을 검증하고, 필요하면 승인을 요청하며, 핸들러를 실행하고, 결과를 기록해 모델에 반환합니다. 핸들러는 제품 고유의 권한 검사를 계속 수행해야 합니다.

### ID를 명시적으로 다루세요

- `ThreadID`는 영속 대화를 식별합니다.
- `TurnID`는 사용자에게 보이는 하나의 턴을 식별합니다.
- `RunID`는 한 번의 구체적인 Provider 실행을 식별합니다.
- `PromptScopeID`는 프롬프트 캐시와 Provider 원장의 재사용 경계입니다.

이 ID들은 의도적으로 서로 다릅니다. 나중에 호스트 소유의 보류 중인 도구 작업을 정산할 때는 UI, 감사, 표시용 ID가 아니라 기록해 둔 Floret `ThreadID`, `TurnID`, `RunID`를 사용해야 합니다.

### 엔진 내부가 아닌 사실을 표시하세요

`runtime.EventSink`로 정제된 수명 주기 이벤트를 받고, `observation` DTO로 컨텍스트 압력, 압축, 활동 타임라인을 렌더링합니다. 이벤트는 호스트 렌더링과 진단을 위한 것이며 시크릿 저장소나 제품 데이터베이스를 대신하지 않습니다.

영속적인 표시 투영이 필요하다면 `ThreadTurnProjection`과 공개 detail API를 사용하세요. Floret 저장소 테이블을 직접 읽거나 호스트에서 Provider가 보는 기록을 다시 만들지 마세요.

### 확신을 갖고 테스트하세요

Fake Provider를 사용하면 실제 모델 호출 없이 도구 동작, 승인 흐름, 재시도, 컨텍스트 압력, 호스트 UI 투영을 테스트할 수 있습니다.

```bash
go test ./...
go run ./cmd/floret-test-ui
```

로컬 테스트 콘솔은 기여자가 Fake Provider 세션, 정제된 이벤트, 도구 시나리오, 호스팅 하위 스레드를 살펴보기 위한 도구입니다. 다운스트림 통합 표면이 아닙니다.

## 다운스트림 통합 표면

다운스트림 애플리케이션은 다음 공개 패키지만 import해야 합니다.

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

`internal/` 아래는 구현 세부 사항입니다. API의 기준은 [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime)이며, 기여자는 [OKF 지식 번들](okf/index.md)에서 런타임 아키텍처와 용어를 확인할 수 있습니다.

## 라이선스

Floret은 [MIT License](LICENSE)로 제공됩니다.
