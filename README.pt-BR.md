# Floret v6

A documentacao canonica da v6, os exemplos de API, o SPI de armazenamento e a migracao estao em [README.md](README.md). Esta pagina nao duplica exemplos para nao preservar uma superficie de API obsoleta.

Modulo: `github.com/floegence/floret/v6`. O Floret v6 e a unica fonte de verdade do ciclo de vida admitido do Agent. Usa exclusivamente `provider.Gateway`, mantem `runtime.Host` na raiz de composicao e expoe um `ThreadService` tipado. `runtime.Open` migra automaticamente os estados de dominio compativeis gerenciados pelo Floret.
