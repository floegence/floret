# Floret

<!-- readme-locales:start -->
<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <a href="README.ja-JP.md">日本語</a> |
  <a href="README.ko-KR.md">한국어</a> |
  <a href="README.de-DE.md">Deutsch</a> |
  <a href="README.fr-FR.md">Français</a> |
  <a href="README.es-ES.md">Español</a> |
  <strong>Português do Brasil</strong> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>O runtime do agente fica com o Floret; o produto continua nas suas mãos.</strong><br />
  Conversas duráveis, execução de ferramentas, ciclo de vida de contexto e fatos de runtime observáveis para aplicações Go.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Referência Go" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="Licença" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Versão do Go" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

![Runtime Floret para aplicações de agentes de IA](okf/assets/readme/floret-agent-app-whiteboard.png)

## Por que Floret?

Fazer um modelo chamar uma ferramenta é só o começo de um produto com agentes. Em produção, você também precisa de um loop de modelo resiliente, histórico de conversa durável, execução de ferramentas com aprovação, gestão de contextos longos, trabalho recuperável e fatos de runtime que a interface possa apresentar com segurança.

Floret entrega essa base de execução sem tomar para si o produto ao redor. Não é uma UI de agentes, um grafo de workflow nem um framework de orquestração multiagente. É a camada Go por trás da experiência que você está criando.

Essa diferença importa quando o produto precisa ser realmente seu. Interface, sistema de identidade, modelo de permissões, roteamento de modelos, segredos, modelo de dados e ferramentas de domínio continuam totalmente sob seu controle. Floret assume o ciclo de vida difícil da execução de agentes.

### Para produtos que não cabem em uma receita pronta

- **Você mantém o controle do caminho até o modelo.** Use a configuração integrada ou implemente `runtime.ModelGateway`. Floret conduz as requisições e suas continuações; o transporte e as credenciais continuam sendo do produto.
- **Dê a cada Agent um papel próprio do seu negócio.** Com `config.AgentProfile.SystemPrompt` ou `config.Config.SystemPrompt`, você define papel, tom, cenário de negócio e regras de operação, em vez de entregar um assistente genérico.
- **Ferramentas e instruções acompanham o trabalho.** Registre ferramentas de domínio estritas em `tools.Registry` e atualize ferramentas, capacidades hospedadas, instruções e contexto do host com `runtime.ToolSurfaceProvider` em pontos seguros de uma execução.
- **Conversas viram ativos confiáveis.** O runtime do Floret gerencia threads, turnos, tentativas, forks, subthreads gerenciadas pelo pai e histórico seguro para o provider.
- **A política de aprovação permanece no produto.** Floret entende efeitos, recursos e estados de aprovação genéricos. Seu produto decide quem pode fazer o quê, onde e por quê.
- **A execução fica visível.** Conecte eventos saneados, pressão de contexto, fatos de compactação e timelines de atividade neutras a qualquer UI, sem expor prompts, segredos ou registros internos.
- **Teste o produto, não a sorte do modelo.** O Fake Provider torna os fluxos de agentes determinísticos tanto localmente quanto no CI.

## O controle continua com você

A fronteira do Floret é intencionalmente estreita: Floret possui a mecânica do motor; a aplicação conserva cada decisão de produto.

| O que Floret executa | O que sua aplicação decide |
| --- | --- |
| Loop do provider, tentativas, continuação de ferramentas e estado final | Quando usuários podem iniciar, repetir, interromper ou cancelar trabalho |
| Diário da thread, escopo de prompt, ledger do provider e artefatos de runtime | Usuários, espaços de trabalho, faturamento, metadados e retenção do produto |
| Validação de schema, efeitos genéricos, ciclo de aprovação e projeção de resultado | Autorização, UI de aprovação, ações de domínio e textos para usuários |
| Pressão de contexto, ciclo de compactação e histórico visível ao modelo | Quais dados do produto podem chegar ao modelo e como serão apresentados |
| Eventos saneados e fatos de atividade neutros | Layout, fluxos, controles, roteamento e política de diagnóstico |

Assim, um console de operações, um ambiente de programação, uma ferramenta de suporte e um assistente vertical podem compartilhar um runtime confiável sem se tornarem o mesmo produto.

## Início rápido

Instale os pacotes estáveis para aplicações downstream:

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

Há um exemplo completo e executável no [README em inglês](README.md#quick-start). Se seu produto controla o transporte do modelo, use uma configuração compatível com OpenAI ou forneça um `runtime.ModelGateway`. Para o Floret persistir seus próprios dados de runtime, use `runtime.OpenSQLiteStore(path)`; os dados de produto continuam no seu armazenamento, relacionados por `runtime.ThreadID`.

## Como levar para produção

### Deixe os prompts expressarem a intenção do produto

Floret não impõe uma persona genérica. Defina o papel inicial, o tom, o cenário de negócio e as regras de operação de um Agent com `config.AgentProfile.SystemPrompt` ou `config.Config.SystemPrompt`. O prompt é uma configuração de produto que pertence ao host: um especialista de suporte, um analista de operações, um assistente de programação ou um especialista de domínio podem compartilhar o mesmo runtime sem modificá-lo.

Para comportamento dependente de contexto, faça `ToolSurfaceProvider` devolver uma `runtime.ToolSurface`. Ela pode substituir o prompt de sistema atual junto com a superfície de ferramentas, as capacidades hospedadas e o contexto do host. Isso atende a mudanças de modo do produto, espaço de trabalho, permissões ou etapa de negócio. Floret atualiza essa superfície antes das requisições ao modelo e do dispatch local; uma decisão antiga do modelo não pode ser executada silenciosamente fora de instruções ou políticas de produto mais recentes.

### Dê ao runtime somente a autoridade necessária

Defina ações de domínio com `tools.Registry`. Cada ferramenta tem um JSON Schema estrito e pode descrever efeitos e recursos. Floret valida a chamada, pede aprovação quando preciso, executa o handler, registra o resultado e o devolve ao modelo. O handler ainda precisa aplicar a autorização específica do produto.

### Trate identidades de forma explícita

- `ThreadID` identifica a conversa durável.
- `TurnID` identifica um turno visível ao usuário.
- `RunID` identifica uma execução concreta do provider.
- `PromptScopeID` é a fronteira de reutilização do cache de prompt e do ledger do provider.

Essas identidades são propositalmente distintas. Para liquidar mais tarde um trabalho pendente de ferramenta que pertence ao host, use os `ThreadID`, `TurnID` e `RunID` do Floret que foram registrados, e não um ID de UI, auditoria ou exibição.

### Renderize fatos, não detalhes internos do motor

Forneça um `runtime.EventSink` para receber eventos de ciclo de vida saneados e use os DTOs de `observation` para pressão de contexto, compactação e timelines de atividade. Eventos servem para renderização e diagnóstico no host; não são um cofre de segredos nem substituem o banco de dados do produto.

Quando precisar de uma projeção de exibição durável, use `ThreadTurnProjection` e as APIs públicas de detalhe. Não leia tabelas de armazenamento do Floret diretamente nem reconstrua no host o histórico visível ao provider.

### Teste com confiança

O Fake Provider permite testar comportamento de ferramentas, fluxos de aprovação, tentativas, pressão de contexto e projeções da UI do host sem chamadas a um modelo real:

```bash
go test ./...
go run ./cmd/floret-test-ui
```

O console de teste local é voltado a contribuidores que inspecionam sessões do Fake Provider, eventos saneados, cenários de ferramenta e subthreads hospedadas. Ele não é uma superfície de integração downstream.

## Superfície de integração downstream

Aplicações downstream devem importar apenas estes pacotes públicos:

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

Tudo sob `internal/` é detalhe de implementação. A referência de API em [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) é a fonte de verdade; o [bundle de conhecimento OKF](okf/index.md) explica a arquitetura e o vocabulário do runtime para contribuidores.

## Licença

Floret é distribuído sob a [licença MIT](LICENSE).
