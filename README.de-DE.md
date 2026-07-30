# Floret v3

Die verbindliche v3-Dokumentation, API-Beispiele, Storage-SPI- und Migrationshinweise stehen in [README.md](README.md). Diese Seite dupliziert bewusst keine Codebeispiele, damit keine veraltete API-Oberflaeche bestehen bleibt.

Modul: `github.com/floegence/floret/v3`. Floret v3 ist die einzige Quelle fuer den aufgenommenen Agent-Lebenszyklus. Es verwendet ausschliesslich `provider.Gateway`, einen nur im Composition Root gehaltenen `runtime.Host`, von Floret vergebene Identitaeten und schmale, identitaetsgebundene Handles. Der Startpfad migriert niemals automatisch und enthaelt keinen Legacy-Decoder.
