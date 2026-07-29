# Floret v2

La documentation v2 canonique, les exemples d'API, le SPI de stockage et la migration se trouvent dans [README.md](README.md). Cette page ne duplique pas les exemples afin de ne conserver aucune surface v1 obsolete.

Module : `github.com/floegence/floret/v2`. Floret v2 utilise exclusivement `provider.Gateway`, conserve `runtime.Host` uniquement dans la racine de composition, construit des `runtime.Agent` immuables et emet des handles etroits lies aux identites. Le demarrage ne migre jamais automatiquement.
