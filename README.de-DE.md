# Floret v2

Die verbindliche v2-Dokumentation, API-Beispiele, Storage-SPI- und Migrationshinweise stehen in [README.md](README.md). Diese Seite dupliziert bewusst keine Codebeispiele, damit keine veraltete v1-Oberflaeche bestehen bleibt.

Modul: `github.com/floegence/floret/v2`. Floret v2 verwendet ausschliesslich `provider.Gateway`, einen nur im Composition Root gehaltenen `runtime.Host`, unveraenderliche `runtime.Agent`-Werte und identitaetsgebundene schmale Handles. Der Startpfad migriert niemals automatisch.
