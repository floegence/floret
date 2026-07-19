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
  <strong>Español</strong> |
  <a href="README.pt-BR.md">Português do Brasil</a> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>Floret se ocupa del runtime del agente; el producto sigue siendo tuyo.</strong><br />
  Conversaciones duraderas, ejecución de herramientas, ciclo de vida del contexto y hechos de runtime observables para aplicaciones Go.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Referencia de Go" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="Licencia" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Versión de Go" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

![Runtime Floret para aplicaciones de agentes de IA](okf/assets/readme/floret-agent-app-whiteboard.png)

## Por qué Floret

Conseguir que un modelo llame a una herramienta es apenas el comienzo de un producto con agentes. En producción también necesitas un bucle de modelo resistente, un registro de conversación duradero, ejecución de herramientas con aprobaciones, gestión de contextos extensos, trabajo recuperable y hechos de runtime que la interfaz pueda mostrar con confianza.

Floret proporciona ese runtime sin apropiarse del producto que lo rodea. No es una UI de agentes, un grafo de flujos de trabajo ni un framework de orquestación multiagente. Es la capa de Go que sostiene la experiencia que estás creando.

La distinción importa cuando el producto debe tener identidad propia. La interfaz, el sistema de identidad, el modelo de permisos, el enrutamiento de modelos, los secretos, el modelo de datos y las herramientas de dominio siguen enteramente bajo tu control. Floret asume el ciclo de vida complejo de la ejecución de agentes.

### Para productos que no pueden conformarse con una receta cerrada

- **Conserva el control de la ruta al modelo.** Usa la configuración integrada o implementa `runtime.ModelGateway`. Floret lleva las solicitudes y su continuación; el transporte y las credenciales siguen siendo del producto.
- **Da a cada Agent un rol propio de tu negocio.** Con `config.AgentProfile.SystemPrompt` o `config.Config.SystemPrompt` defines el rol, el tono, el escenario de negocio y las reglas de operación, en lugar de entregar un asistente genérico.
- **Haz que las herramientas y las instrucciones respondan al trabajo.** Registra herramientas de dominio estrictas en `tools.Registry` y actualiza herramientas, capacidades alojadas, instrucciones y contexto del host con `runtime.ToolSurfaceProvider` en puntos seguros de una ejecución.
- **Convierte las conversaciones en activos fiables.** El runtime de Floret gestiona hilos, turnos, reintentos, bifurcaciones, subhilos administrados por el padre e historial seguro para el provider.
- **Mantén la política de aprobación en el producto.** Floret entiende efectos, recursos y estados de aprobación genéricos. Tu producto decide quién puede hacer qué, dónde y por qué.
- **Haz visible la ejecución.** Conecta eventos saneados, presión de contexto, hechos de compactación y líneas de tiempo de actividad neutrales a cualquier UI, sin exponer prompts, secretos ni registros internos.
- **Prueba el producto, no la suerte del modelo.** El Fake Provider vuelve deterministas los flujos de agentes, tanto en local como en CI.

## El control sigue en tus manos

La frontera de Floret es deliberadamente estrecha: Floret posee la mecánica del motor y la aplicación conserva cada decisión de producto.

| Floret ejecuta | Tu aplicación decide |
| --- | --- |
| Bucle del provider, reintentos, continuación de herramientas y estado final | Cuándo los usuarios pueden iniciar, reintentar, interrumpir o cancelar trabajo |
| Diario de hilos, ámbito de prompt, ledger del provider y artefactos de runtime | Usuarios, espacios de trabajo, facturación, metadatos y retención del producto |
| Validación de esquema, efectos genéricos, ciclo de aprobación y proyección de resultados | Autorización, UI de aprobación, acciones de dominio y textos para usuarios |
| Presión de contexto, ciclo de compactación e historial visible para el modelo | Qué datos del producto pueden llegar al modelo y cómo se presentan |
| Eventos saneados y hechos de actividad neutrales | Diseño, flujos, controles, enrutamiento y política de diagnóstico |

Así, una consola de operaciones, un entorno de programación, una herramienta de soporte y un asistente vertical pueden compartir un runtime sólido sin convertirse en el mismo producto.

## Inicio rápido

Instala los paquetes estables para aplicaciones downstream:

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

Encontrarás un ejemplo completo y ejecutable en el [README en inglés](README.md#quick-start). Si tu producto gestiona el transporte del modelo, usa una configuración compatible con OpenAI o proporciona un `runtime.ModelGateway`. Para que Floret persista sus propios datos de runtime, usa `runtime.OpenSQLiteStore(path)`; los datos de producto siguen en tu almacenamiento, relacionados mediante `runtime.ThreadID`.

## Forma de llevarlo a producción

### Deja que los prompts expresen la intención del producto

Floret no impone una personalidad genérica. Define el rol inicial, el tono, el escenario de negocio y las reglas de operación de un Agent mediante `config.AgentProfile.SystemPrompt` o `config.Config.SystemPrompt`. El prompt es configuración de producto propiedad del host: un especialista de soporte, un analista de operaciones, un asistente de programación o un experto de dominio pueden compartir el mismo runtime sin cambiarlo.

Para comportamiento dependiente del contexto, haz que `ToolSurfaceProvider` devuelva una `runtime.ToolSurface`. Puede sustituir el prompt de sistema actual junto con la superficie de herramientas, las capacidades alojadas y el contexto del host. Sirve para cambios de modo de producto, espacio de trabajo, permisos o etapa de negocio. Floret actualiza esa superficie antes de las solicitudes del modelo y del dispatch local; una decisión antigua del modelo no puede ejecutarse en silencio fuera de instrucciones o políticas de producto más recientes.

### Concede al runtime solo la autoridad que necesita

Define acciones de dominio con `tools.Registry`. Cada herramienta tiene un JSON Schema estricto y puede describir efectos y recursos. Floret valida la llamada, solicita aprobación cuando hace falta, ejecuta el handler, registra el resultado y lo devuelve al modelo. El handler debe seguir aplicando la autorización específica de tu producto.

### Trata las identidades de forma explícita

- `ThreadID` identifica la conversación duradera.
- `TurnID` identifica un turno visible para el usuario.
- `RunID` identifica una ejecución concreta del provider.
- `PromptScopeID` es el límite de reutilización de la caché de prompts y del ledger del provider.

Estas identidades son deliberadamente distintas. Para liquidar más adelante trabajo pendiente de una herramienta que pertenece al host, usa los `ThreadID`, `TurnID` y `RunID` de Floret que registraste, no un ID de UI, auditoría o visualización.

### Muestra hechos, no internals del motor

Proporciona un `runtime.EventSink` para recibir eventos de ciclo de vida saneados y usa los DTO de `observation` para presión de contexto, compactación y líneas de tiempo de actividad. Los eventos están pensados para el renderizado y el diagnóstico del host; no son un almacén de secretos ni sustituyen la base de datos del producto.

Cuando necesites una proyección de visualización duradera, usa `ThreadTurnProjection` y las API públicas de detalle. No leas directamente las tablas de almacenamiento de Floret ni reconstruyas en el host el historial visible para el provider.

### Prueba con confianza

El Fake Provider permite probar el comportamiento de herramientas, flujos de aprobación, reintentos, presión de contexto y proyecciones de UI del host sin llamadas a un modelo real:

```bash
go test ./...
go run ./cmd/floret-test-ui
```

La consola de pruebas local es una herramienta para colaboradores que inspeccionan sesiones de Fake Provider, eventos saneados, escenarios de herramientas y subhilos alojados. No es una superficie de integración downstream.

## Referencia

Las aplicaciones downstream solo deben importar estos paquetes públicos:

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

Todo lo que está bajo `internal/` es un detalle de implementación. La referencia de API en [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) es la fuente de verdad; el [bundle de conocimiento OKF](okf/index.md) explica a los colaboradores la arquitectura y el vocabulario del runtime.

## Licencia

Floret se distribuye bajo la [licencia MIT](LICENSE).
