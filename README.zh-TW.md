# Floret v3

v3 的正式 API、程式範例、儲存 SPI 與遷移說明位於 [README.md](README.md)。本頁不重複程式範例，以避免保留過時的 API 介面。

模組路徑為 `github.com/floegence/floret/v4`。v3 是已接納 Agent 生命週期的唯一事實來源；所有模型執行只經過 `provider.Gateway`，`runtime.Host` 只保存在 composition root，生命週期身分由 Floret 分配，所有窄句柄都綁定明確身分。啟動流程絕不自動遷移，也不包含 legacy decoder。
