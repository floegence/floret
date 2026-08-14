# Floret v3

v3 の正式な API、コード例、ストレージ SPI、移行手順は [README.md](README.md) にあります。このページでは古い API を残さないため、コード例を重複掲載しません。

モジュールは `github.com/floegence/floret/v4` です。v3 は受理された Agent ライフサイクルの唯一の信頼できる情報源です。すべてのモデル実行を `provider.Gateway` に統一し、`runtime.Host` を composition root だけに保持し、ライフサイクル ID を Floret が割り当て、ID に束縛された狭いハンドルを使用します。起動時の自動移行や legacy decoder はありません。
