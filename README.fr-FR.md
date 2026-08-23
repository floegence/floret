# Floret v5

La documentation v5 canonique, les exemples d'API, le SPI de stockage et la migration se trouvent dans [README.md](README.md). Cette page ne duplique pas les exemples afin de ne conserver aucune surface d'API obsolete.

Module : `github.com/floegence/floret/v5`. Floret v5 est l'unique source de verite du cycle de vie Agent admis. Il utilise exclusivement `provider.Gateway`, conserve `runtime.Host` dans la racine de composition et expose un `ThreadService` type. `runtime.Open` migre automatiquement les etats de domaine compatibles geres par Floret.
