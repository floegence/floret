# Floret v3

La documentation v3 canonique, les exemples d'API, le SPI de stockage et la migration se trouvent dans [README.md](README.md). Cette page ne duplique pas les exemples afin de ne conserver aucune surface d'API obsolete.

Module : `github.com/floegence/floret/v3`. Floret v3 est l'unique source de verite du cycle de vie Agent admis. Il utilise exclusivement `provider.Gateway`, conserve `runtime.Host` uniquement dans la racine de composition, attribue les identites du cycle de vie et emet des handles etroits lies aux identites. Le demarrage ne migre jamais automatiquement et ne contient aucun decodeur legacy.
