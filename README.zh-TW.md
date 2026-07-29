# Floret v2

v2 的正式 API、程式範例、儲存 SPI 與遷移說明位於 [README.md](README.md)。本頁不重複程式範例，以避免保留過時的 v1 介面。

模組路徑為 `github.com/floegence/floret/v2`。v2 的所有模型執行只經過 `provider.Gateway`，`runtime.Host` 只保存在 composition root，`runtime.Agent` 建構後不可變，所有窄句柄都綁定明確身分。啟動流程絕不自動遷移。
