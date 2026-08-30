# Floret v6

Die verbindliche v6-Dokumentation, API-Beispiele, Storage-SPI- und Migrationshinweise stehen in [README.md](README.md). Diese Seite dupliziert bewusst keine Codebeispiele, damit keine veraltete API-Oberflaeche bestehen bleibt.

Modul: `github.com/floegence/floret/v6`. Floret v6 ist die einzige Quelle fuer den aufgenommenen Agent-Lebenszyklus. Es verwendet ausschliesslich `provider.Gateway`, einen nur im Composition Root gehaltenen `runtime.Host` und einen typisierten `ThreadService`. `runtime.Open` migriert unterstuetzte, von Floret verwaltete Domain-Zustaende automatisch.
