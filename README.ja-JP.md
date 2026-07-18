# Floret

<!-- readme-locales:start -->
<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <strong>日本語</strong> |
  <a href="README.ko-KR.md">한국어</a> |
  <a href="README.de-DE.md">Deutsch</a> |
  <a href="README.fr-FR.md">Français</a> |
  <a href="README.es-ES.md">Español</a> |
  <a href="README.pt-BR.md">Português do Brasil</a> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>エージェントの実行基盤は Floret に。プロダクトの主導権は、あなたに。</strong><br />
  永続的な会話、ツール実行、コンテキストのライフサイクル、観測可能な実行時情報を Go アプリケーションへ。
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Go リファレンス" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="ライセンス" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go バージョン" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

![Floret AI エージェントアプリケーションの実行基盤](okf/assets/readme/floret-agent-app-whiteboard.png)

## Floret を選ぶ理由

モデルにツールを呼び出させるだけでは、エージェント製品は完成しません。本番では、堅牢なモデルループ、永続化された会話記録、承認を伴うツール実行、長いコンテキストの管理、回復可能な処理、そして UI が安心して表示できる実行時の事実が必要です。

Floret はその実行基盤を提供しますが、プロダクトを支配しません。エージェント UI でも、ワークフローグラフでも、マルチエージェントのオーケストレーターでもありません。あなたが構築する体験を下支えする Go のランタイムです。

独自性が必要なプロダクトにとって、この境界は重要です。UI、ID 基盤、権限モデル、モデルルーティング、シークレット、データモデル、ドメインツールは引き続きあなたのものです。Floret は難しいエージェント実行のライフサイクルを引き受けます。

### 既製の型に収まらないプロダクトのために

- **モデルへの経路は手元に残ります。** 組み込み設定を使うことも、`runtime.ModelGateway` を実装することもできます。Floret がリクエストと継続のライフサイクルを担い、トランスポートと認証情報は製品側が管理します。
- **業務に根ざした役割を Agent に与えられます。** `config.AgentProfile.SystemPrompt` または `config.Config.SystemPrompt` で、役割、語り口、業務シナリオ、運用ルールを定義できます。ありふれた汎用アシスタントを出荷する必要はありません。
- **仕事の状況に合わせてツールと指示を変えられます。** `tools.Registry` に厳密なドメインツールを登録し、`runtime.ToolSurfaceProvider` で実行中の安全な時点にツール、ホスト型機能、指示、ホストコンテキストを更新できます。
- **会話を信頼できる資産にします。** Floret ランタイムがスレッド、ターン、再試行、フォーク、親が管理する子スレッド、プロバイダーにとって安全な履歴を管理します。
- **承認ポリシーは製品側に残ります。** Floret は一般的な副作用、リソース、承認状態を扱います。誰が、何を、どこで、なぜ実行できるかは製品が決めます。
- **実行中のことを可視化できます。** サニタイズ済みイベント、コンテキスト圧、圧縮の事実、中立的なアクティビティタイムラインを、プロンプトやシークレット、内部ストレージを露出せずに任意の UI へ渡せます。
- **モデルに頼らずテストできます。** Fake Provider により、ローカルと CI でエージェントフローを決定的にテストできます。

## 主導権を手放さない境界

Floret の責務は意図的に絞られています。エンジンの仕組みは Floret、プロダクトの判断はアプリケーションのものです。

| Floret が担うこと | アプリケーションが決めること |
| --- | --- |
| Provider ループ、再試行、ツール継続、終了状態 | ユーザーが処理を開始、再試行、中断、キャンセルできる条件 |
| スレッドジャーナル、プロンプトスコープ、Provider 台帳、実行時アーティファクト | ユーザー、ワークスペース、課金、プロダクトメタデータ、保持方針 |
| ツールスキーマ検証、一般的な副作用、承認ライフサイクル、結果投影 | 認可、承認 UI、ドメイン操作、ユーザー向けの文言 |
| コンテキスト圧、圧縮ライフサイクル、モデルに見える履歴 | モデルへ渡してよい製品データとその表示方法 |
| サニタイズ済みイベントと中立的なアクティビティ情報 | レイアウト、ワークフロー、操作、ルーティング、診断方針 |

この境界があるため、運用コンソール、コーディング環境、サポートツール、業界特化アシスタントは同じ信頼できる基盤を共有しながら、同じ製品になる必要がありません。

## クイックスタート

下流アプリケーション向けの安定パッケージをインストールします。

```bash
go get github.com/floegence/floret/config github.com/floegence/floret/runtime github.com/floegence/floret/tools github.com/floegence/floret/observation
```

```go
store := runtime.NewMemoryStore()
defer store.Close()
bootstrap, err := runtime.NewHostBootstrap(store)
if err != nil { /* handle error */ }
threadCreator, err := runtime.NewThreadCreateHost(bootstrap, nil)
if err != nil { /* handle error */ }

turnFactory, err := runtime.NewTurnExecutionHostFactory(bootstrap)
if err != nil { /* handle error */ }
turnHost, err := turnFactory.NewHost(runtime.TurnExecutionHostOptions{
	ThreadID: "thread-1",
	Config: config.Config{
		Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "Hello from Floret.",
		AgentProfile: config.AgentProfile{ID: "support-agent", Name: "Support Agent"},
	},
})
if err != nil { /* handle error */ }

thread, err := threadCreator.CreateThread(ctx, runtime.CreateThreadRequest{ThreadID: "thread-1"})
result, err := turnHost.RunTurn(ctx, runtime.RunTurnRequest{
	ThreadID: thread.ID, TurnID: "turn-1", RunID: "run-1",
	Input: runtime.TurnInput{Text: "Welcome a new customer in one sentence."},
})
```

そのまま実行できる完全な例は [英語 README](README.md#quick-start) にあります。モデルトランスポートを製品側で管理する場合は OpenAI-compatible 設定を使うか、`runtime.ModelGateway` を渡します。Floret に実行時データを永続化させる場合は `runtime.OpenSQLiteStore(path)` を使います。プロダクトデータは引き続き自分のストアに置き、`runtime.ThreadID` で関連付けます。

## 本番環境での組み込み方

### プロンプトにプロダクトの意図を載せる

Floret は汎用的な人格を押し付けません。`config.AgentProfile.SystemPrompt` または `config.Config.SystemPrompt` で、Agent の初期の役割、語り口、業務シナリオ、運用ルールを定義します。プロンプトはホストが所有する製品設定です。サポートの専門家、運用アナリスト、コーディングアシスタント、業界の専門家は、ランタイムを変えずに同じ基盤を共有できます。

状況に応じて振る舞いを変える場合は、`ToolSurfaceProvider` から `runtime.ToolSurface` を返します。ツールサーフェス、ホスト型機能、ホストコンテキストとともに現在のシステムプロンプトを置き換えられます。製品モード、ワークスペース、権限、業務段階の変化に適しています。Floret はモデルリクエストとローカルディスパッチの前にサーフェスを更新するため、古いモデル判断が新しい指示や製品ポリシーの外で静かに実行されることはありません。

### ランタイムには必要な権限だけを渡す

ドメイン操作は `tools.Registry` で定義します。各ツールは厳密な JSON Schema を持ち、副作用とリソースを記述できます。Floret は呼び出しを検証し、必要なら承認を求め、ハンドラーを実行し、結果を記録してモデルに返します。ハンドラーでは、製品固有の認可を必ず実施してください。

### ID を明示的に扱う

- `ThreadID` は永続会話を識別します。
- `TurnID` はユーザーに見える一つのターンを識別します。
- `RunID` は具体的な一回の Provider 実行を識別します。
- `PromptScopeID` はプロンプトキャッシュと Provider 台帳の再利用境界です。

これらは意図的に別物です。後でホスト所有の保留中ツール処理を確定する際は、UI、監査、表示用の ID ではなく、記録しておいた Floret の `ThreadID`、`TurnID`、`RunID` を使います。

### エンジン内部ではなく、事実を表示する

`runtime.EventSink` でサニタイズ済みのライフサイクルイベントを受け取り、`observation` DTO でコンテキスト圧、圧縮、アクティビティタイムラインを表示します。イベントはホストの描画と診断のためのものであり、シークレットストアでもプロダクトデータベースの代替でもありません。

永続的な表示投影が必要な場合は、`ThreadTurnProjection` と公開 detail API を使います。Floret のストレージテーブルを直接読んだり、ホスト側で Provider 可視の履歴を組み直したりしないでください。

### 確信を持ってテストする

Fake Provider なら、実モデルの呼び出しなしでツール動作、承認フロー、再試行、コンテキスト圧、ホスト UI 投影をテストできます。

```bash
go test ./...
go run ./cmd/floret-test-ui
```

ローカルテストコンソールは、Fake Provider セッション、サニタイズ済みイベント、ツールシナリオ、ホストされた子スレッドを確認するためのコントリビューター向け機能です。下流の統合インターフェースではありません。

## リファレンス

下流アプリケーションが import すべき公開パッケージは次の四つだけです。

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

`internal/` 配下は実装詳細です。API の正本は [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) です。コントリビューターは [OKF ナレッジバンドル](okf/index.md) で実行基盤のアーキテクチャーと用語を確認できます。

## ライセンス

Floret は [MIT License](LICENSE) の下で提供されています。
