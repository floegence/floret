# Floret v2

A documentacao canonica da v2, os exemplos de API, o SPI de armazenamento e a migracao estao em [README.md](README.md). Esta pagina nao duplica exemplos para nao preservar uma superficie v1 obsoleta.

Modulo: `github.com/floegence/floret/v2`. O Floret v2 usa exclusivamente `provider.Gateway`, mantem `runtime.Host` apenas na raiz de composicao, constroi `runtime.Agent` imutaveis e emite handles estreitos vinculados a identidades. A inicializacao nunca migra dados automaticamente.
