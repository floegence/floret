# Floret v3

La documentacion canonica de v3, los ejemplos de API, el SPI de almacenamiento y la migracion estan en [README.md](README.md). Esta pagina no duplica ejemplos para evitar conservar una superficie de API obsoleta.

Modulo: `github.com/floegence/floret/v3`. Floret v3 es la unica fuente de verdad del ciclo de vida admitido del Agent. Usa exclusivamente `provider.Gateway`, mantiene `runtime.Host` solo en la raiz de composicion, asigna las identidades del ciclo de vida y entrega handles estrechos ligados a identidades. El arranque nunca migra datos automaticamente ni incluye un decoder legacy.
