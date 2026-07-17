# Floret

<!-- readme-locales:start -->
<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <a href="README.ja-JP.md">日本語</a> |
  <a href="README.ko-KR.md">한국어</a> |
  <strong>Deutsch</strong> |
  <a href="README.fr-FR.md">Français</a> |
  <a href="README.es-ES.md">Español</a> |
  <a href="README.pt-BR.md">Português do Brasil</a> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>Floret betreibt den Agenten. Dein Produkt bleibt deins.</strong><br />
  Dauerhafte Gespräche, Tool-Ausführung, Kontext-Lebenszyklus und beobachtbare Laufzeitfakten für Go-Anwendungen.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Go-Referenz" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="Lizenz" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Go-Version" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

![Floret-Laufzeit für KI-Agent-Anwendungen](okf/assets/readme/floret-agent-app-whiteboard.png)

## Warum Floret?

Ein Modell ein Tool aufrufen zu lassen, ist erst der Anfang eines Agentenprodukts. Im Betrieb braucht es zusätzlich eine belastbare Modellschleife, einen dauerhaften Gesprächsverlauf, Tool-Ausführung mit Freigaben, Management langer Kontexte, wiederherstellbare Arbeit und Laufzeitfakten, die die UI zuverlässig darstellen kann.

Floret liefert diese Laufzeit, ohne das Produkt darum herum zu vereinnahmen. Es ist weder eine Agenten-UI noch ein Workflow-Graph oder Multi-Agenten-Framework. Es ist die Go-Schicht hinter der Erfahrung, die du entwickelst.

Das ist entscheidend, wenn ein Produkt wirklich eigenständig bleiben muss. Oberfläche, Identitätssystem, Berechtigungsmodell, Modellrouting, Geheimnisse, Datenmodell und Domänen-Tools bleiben vollständig unter deiner Kontrolle. Floret übernimmt den schwierigen Lebenszyklus der Agentenausführung.

### Für Produkte, die keine Vorgaben akzeptieren können

- **Der Modellzugang bleibt bei dir.** Nutze die eingebaute Konfiguration oder implementiere `runtime.ModelGateway`. Floret steuert Anfrage und Fortsetzung, Transport und Zugangsdaten bleiben beim Produkt.
- **Gib jedem Agenten eine Rolle, die zu deinem Geschäft passt.** Mit `config.AgentProfile.SystemPrompt` oder `config.Config.SystemPrompt` definierst du Rolle, Ton, Geschäftsszenario und Arbeitsregeln, statt einen beliebigen Standardassistenten auszuliefern.
- **Tools und Anweisungen passen sich der Arbeit an.** Registriere strikte Domänen-Tools in `tools.Registry` und erneuere mit `runtime.ToolSurfaceProvider` Tools, gehostete Fähigkeiten, Anweisungen und Host-Kontext an sicheren Punkten eines Laufs.
- **Gespräche werden zu belastbaren Assets.** `runtime.Host` verwaltet Threads, Turns, Wiederholungen, Forks, elternverwaltete Unterthreads und provider-sicheren Verlauf.
- **Freigaberegeln bleiben im Produkt.** Floret kennt allgemeine Effekte, Ressourcen und Freigabestatus. Wer was, wo und warum tun darf, entscheidest du.
- **Laufzeitverhalten wird sichtbar.** Sanitisierte Ereignisse, Kontextdruck, Kompaktierungsfakten und neutrale Aktivitäts-Timelines lassen sich ohne Prompts, Geheimnisse oder interne Speicherzeilen in jede UI einbinden.
- **Teste das Produkt statt das Zufallsverhalten eines Modells.** Der Fake Provider macht Agentenflüsse lokal und in CI deterministisch.

## Du behältst die Hoheit

Die Grenze von Floret ist bewusst eng: Floret besitzt die Mechanik der Engine, die Anwendung besitzt jede Produktentscheidung.

| Floret führt aus | Deine Anwendung entscheidet |
| --- | --- |
| Provider-Schleife, Wiederholungen, Tool-Fortsetzung und Endzustand | Wann Nutzer Arbeit starten, wiederholen, unterbrechen oder abbrechen dürfen |
| Thread-Journal, Prompt-Scope, Provider-Ledger und Laufzeit-Artefakte | Nutzer, Workspaces, Abrechnung, Produktmetadaten und Aufbewahrung |
| Tool-Schema-Prüfung, allgemeine Effekte, Freigabe-Lebenszyklus und Ergebnisprojektion | Autorisierung, Freigabe-UI, Domänenaktionen und nutzernahe Texte |
| Kontextdruck, Kompaktierungs-Lebenszyklus und modell-sichtbarer Verlauf | Welche Produktdaten an das Modell dürfen und wie sie angezeigt werden |
| Sanitisierte Ereignisse und neutrale Aktivitätsfakten | Layout, Workflows, Bedienelemente, Routing und Diagnoserichtlinien |

So können eine Betriebskonsole, eine Coding-Umgebung, ein Support-Tool oder ein branchenspezifischer Assistent dieselbe verlässliche Laufzeit nutzen, ohne zu demselben Produkt zu werden.

## Schnellstart

Installiere die stabilen Pakete für Downstream-Anwendungen:

```bash
go get github.com/floegence/floret/config github.com/floegence/floret/runtime github.com/floegence/floret/tools github.com/floegence/floret/observation
```

```go
store := runtime.NewMemoryStore()
defer store.Close()

host, err := runtime.NewHost(runtime.HostOptions{
	Config: config.Config{
		Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "Hello from Floret.",
		AgentProfile: config.AgentProfile{ID: "support-agent", Name: "Support Agent"},
	},
	Store: store,
})
if err != nil { /* handle error */ }

thread, err := host.StartThread(ctx, runtime.StartThreadRequest{ThreadID: "thread-1"})
result, err := host.RunTurn(ctx, runtime.RunTurnRequest{
	ThreadID: thread.ID, TurnID: "turn-1", RunID: "run-1",
	Input: "Welcome a new customer in one sentence.",
})
```

Ein vollständiges, direkt ausführbares Beispiel steht im [englischen README](README.md#quick-start). Wenn dein Produkt den Modelltransport besitzt, verwende eine OpenAI-kompatible Konfiguration oder übergib ein `runtime.ModelGateway`. Soll Floret eigene Laufzeitdaten dauerhaft speichern, nutze `runtime.OpenSQLiteStore(path)`. Produktdaten bleiben in deinem Store und werden über `runtime.ThreadID` verknüpft.

## Form für den Produktiveinsatz

### Lass Prompts die Produktabsicht tragen

Floret schreibt keine generische Persona vor. Mit `config.AgentProfile.SystemPrompt` oder `config.Config.SystemPrompt` gibst du einem Agenten seine anfängliche Rolle, seinen Ton, sein Geschäftsszenario und seine Arbeitsregeln. Der Prompt ist produktseitige Konfiguration: Support-Spezialisten, Betriebsanalysten, Coding-Assistenten oder Branchenexperten können dieselbe Laufzeit nutzen, ohne die Laufzeit selbst zu verändern.

Für kontextabhängiges Verhalten lässt ein `ToolSurfaceProvider` eine `runtime.ToolSurface` zurückgeben. Sie kann neben Tool-Oberfläche, gehosteten Fähigkeiten und Host-Kontext auch den aktuellen System-Prompt ersetzen. Das passt zu Änderungen von Produktmodus, Workspace, Berechtigungen oder Geschäftsphase. Floret aktualisiert die Oberfläche vor Modellanfragen und lokaler Ausführung, sodass eine ältere Modellentscheidung nicht unbemerkt außerhalb neuer Anweisungen oder Produktrichtlinien laufen kann.

### Gib der Laufzeit nur die notwendige Berechtigung

Domänenaktionen werden mit `tools.Registry` definiert. Jedes Tool hat ein striktes JSON-Schema und kann Effekte und Ressourcen beschreiben. Floret validiert den Aufruf, fordert bei Bedarf eine Freigabe an, führt den Handler aus, protokolliert das Ergebnis und gibt es an das Modell zurück. Der Handler muss weiterhin die produktspezifische Autorisierung erzwingen.

### Arbeite mit expliziten Identitäten

- `ThreadID` identifiziert das dauerhafte Gespräch.
- `TurnID` identifiziert einen nutzersichtbaren Turn.
- `RunID` identifiziert eine konkrete Provider-Ausführung.
- `PromptScopeID` ist die Wiederverwendungsgrenze für Prompt-Cache und Provider-Ledger.

Diese Identitäten sind absichtlich getrennt. Wird später eine ausstehende, host-eigene Tool-Arbeit abgeschlossen, müssen die gespeicherten Floret-Identitäten `ThreadID`, `TurnID` und `RunID` verwendet werden, nicht UI-, Audit- oder Anzeige-IDs.

### Zeige Fakten, nicht Engine-Interna

Übergib einen `runtime.EventSink`, um sanitisierte Lebenszyklusereignisse zu erhalten, und nutze die `observation`-DTOs für Kontextdruck, Kompaktierung und Aktivitäts-Timelines. Ereignisse sind für die Darstellung und Diagnose im Host gedacht, nicht als Secret Store oder Ersatz für die Produktdatenbank.

Für eine dauerhafte Anzeigeprojektion verwende `ThreadTurnProjection` und die öffentlichen Detail-APIs. Lies weder Florets Speichertabellen direkt noch baue provider-sichtbaren Verlauf im Host nach.

### Teste mit Sicherheit

Mit dem Fake Provider lassen sich Tool-Verhalten, Freigabeflüsse, Wiederholungen, Kontextdruck und Host-UI-Projektionen ohne echte Modellaufrufe testen:

```bash
go test ./...
go run ./cmd/floret-test-ui
```

Die lokale Testkonsole dient Beitragenden zur Prüfung von Fake-Provider-Sitzungen, sanitisierten Ereignissen, Tool-Szenarien und gehosteten Unterthreads. Sie ist keine Downstream-Integrationsoberfläche.

## Referenz

Downstream-Anwendungen sollen ausschließlich diese öffentlichen Pakete importieren:

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

Alles unter `internal/` ist Implementierungsdetail. Die API-Referenz auf [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) ist maßgeblich; im [OKF-Wissenspaket](okf/index.md) finden Beitragende Architektur und Vokabular der Laufzeit.

## Lizenz

Floret steht unter der [MIT License](LICENSE).
