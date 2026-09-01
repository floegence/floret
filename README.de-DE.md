# Floret v7

Die verbindliche v7-Dokumentation, API-Beispiele, Storage-SPI- und Migrationshinweise stehen in [README.md](README.md). Diese Seite dupliziert bewusst keine Codebeispiele, damit keine veraltete API-Oberflaeche bestehen bleibt.

Modul: `github.com/floegence/floret/v7`. Floret v7 ist die einzige Quelle fuer den aufgenommenen Agent-Lebenszyklus. Es verwendet ausschliesslich `provider.Gateway`, einen nur im Composition Root gehaltenen `runtime.Host` und einen typisierten `ThreadService`. `runtime.Open` migriert unterstuetzte, von Floret verwaltete Domain-Zustaende automatisch.
