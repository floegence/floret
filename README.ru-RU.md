# Floret v2

Каноническая документация v2, примеры API, storage SPI и процедура миграции находятся в [README.md](README.md). Эта страница намеренно не дублирует примеры, чтобы не сохранять устаревший API v1.

Модуль: `github.com/floegence/floret/v2`. Floret v2 использует только `provider.Gateway`, хранит `runtime.Host` исключительно в composition root, создает неизменяемые `runtime.Agent` и выдает узкие handles, привязанные к идентификаторам. Автоматической миграции при запуске нет.
