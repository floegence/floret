# Floret v5

v5 的正式 API、程式範例、儲存 SPI 與遷移說明位於 [README.md](README.md)。本頁不重複程式範例，以避免保留過時的 API 介面。

模組路徑為 `github.com/floegence/floret/v5`。v5 是已接納 Agent 生命週期的唯一事實來源；所有模型執行只經過 `provider.Gateway`，`runtime.Host` 只保存在 composition root，並公開 typed `ThreadService`。`runtime.Open` 會自動遷移 Floret 自有且受支援的 domain state。
