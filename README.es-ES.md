# Floret v7

La documentacion canonica de v7, los ejemplos de API, el SPI de almacenamiento y la migracion estan en [README.md](README.md). Esta pagina no duplica ejemplos para evitar conservar una superficie de API obsoleta.

Modulo: `github.com/floegence/floret/v7`. Floret v7 es la unica fuente de verdad del ciclo de vida admitido del Agent. Usa exclusivamente `provider.Gateway`, mantiene `runtime.Host` en la raiz de composicion y expone un `ThreadService` tipado. `runtime.Open` migra automaticamente los estados de dominio compatibles administrados por Floret.
