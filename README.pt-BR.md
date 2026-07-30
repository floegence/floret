# Floret v3

A documentacao canonica da v3, os exemplos de API, o SPI de armazenamento e a migracao estao em [README.md](README.md). Esta pagina nao duplica exemplos para nao preservar uma superficie de API obsoleta.

Modulo: `github.com/floegence/floret/v3`. O Floret v3 e a unica fonte de verdade do ciclo de vida admitido do Agent. Usa exclusivamente `provider.Gateway`, mantem `runtime.Host` apenas na raiz de composicao, atribui as identidades do ciclo de vida e emite handles estreitos vinculados a identidades. A inicializacao nunca migra dados automaticamente nem inclui um decoder legacy.
