# Floret v2

v2 の正式な API、コード例、ストレージ SPI、移行手順は [README.md](README.md) にあります。このページでは古い v1 API を残さないため、コード例を重複掲載しません。

モジュールは `github.com/floegence/floret/v2` です。v2 はすべてのモデル実行を `provider.Gateway` に統一し、`runtime.Host` を composition root だけに保持し、不変の `runtime.Agent` と ID に束縛された狭いハンドルを使用します。起動時の自動移行はありません。
