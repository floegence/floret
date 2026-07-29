# Floret v2

La documentacion canonica de v2, los ejemplos de API, el SPI de almacenamiento y la migracion estan en [README.md](README.md). Esta pagina no duplica ejemplos para evitar conservar una superficie v1 obsoleta.

Modulo: `github.com/floegence/floret/v2`. Floret v2 usa exclusivamente `provider.Gateway`, mantiene `runtime.Host` solo en la raiz de composicion, construye `runtime.Agent` inmutables y entrega handles estrechos ligados a identidades. El arranque nunca migra datos automaticamente.
