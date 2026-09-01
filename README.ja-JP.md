# Floret v7

v7 の正式な API、コード例、ストレージ SPI、移行手順は [README.md](README.md) にあります。このページでは古い API を残さないため、コード例を重複掲載しません。

モジュールは `github.com/floegence/floret/v7` です。v7 は受理された Agent ライフサイクルの唯一の信頼できる情報源です。すべてのモデル実行を `provider.Gateway` に統一し、`runtime.Host` を composition root に保持し、型付き `ThreadService` を公開します。`runtime.Open` は Floret が所有する対応済みドメイン状態を自動移行します。
