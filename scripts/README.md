# Notification Service 測試腳本

## 前置條件

### 安裝工具

```bash
# macOS
brew install kcat websocat jq

# Linux (Debian/Ubuntu)
apt-get install kafkacat websocat jq
```

### 環境變數

```bash
export KAFKA_BOOTSTRAP="skafka:9092"  # Kafka 位址
export KAFKA_TOPIC="results"          # Kafka Topic
export NS_HOST="localhost"            # NS 主機
export NS_PORT="8080"                 # NS 端口
```

## 快速開始

### 自動化 E2E 測試 (推薦)

```bash
# 快速測試
./e2e-test.sh

# 詳細輸出
./e2e-test.sh --verbose

# 執行所有測試案例
./e2e-test.sh --all --verbose
```

### 手動測試

### 手動測試

#### 1. 健康檢查

```bash
./test-notification.sh health
```

#### 2. 完整測試 (Kafka → NS → WebSocket)

**終端機 1: 監聽 WebSocket**
```bash
./ws-listen.sh test-001
```

**終端機 2: 發送 Kafka 訊息**
```bash
./kafka-send.sh test-001 1000 "處理完成"
```

#### 3. 測試不同狀態碼

```bash
# 成功 (1xxx) → INFO log → WebSocket 關閉
./kafka-send.sh evt-success 1000 "處理完成"

# 業務錯誤 (2xxx) → WARN log → WebSocket 關閉
./kafka-send.sh evt-biz-err 2001 "資料不存在"

# 系統錯誤 (3xxx) → ERROR log → WebSocket 關閉
./kafka-send.sh evt-sys-err 3001 "處理超時"
```

## 狀態碼對照表

| 狀態碼 | 說明 | Log Level | WebSocket |
|--------|------|-----------|-----------|
| 1000 | 成功 | INFO | 關閉連線 |
| 1001 | 部分成功 | INFO | 關閉連線 |
| 1002 | 已接受 | INFO | 關閉連線 |
| 2000 | 業務錯誤 | WARN | 關閉連線 |
| 2001 | 資料不存在 | WARN | 關閉連線 |
| 2002 | 驗證失敗 | WARN | 關閉連線 |
| 2003 | 權限不足 | WARN | 關閉連線 |
| 2004 | 重複請求 | WARN | 關閉連線 |
| 3000 | 系統錯誤 | ERROR | 關閉連線 |
| 3001 | 處理超時 | ERROR | 關閉連線 |
| 3002 | 服務不可用 | ERROR | 關閉連線 |
| 3003 | 內部錯誤 | ERROR | 關閉連線 |

> **Note**: 所有 ≥1000 的狀態碼都是終態，伺服器會在推送通知後主動關閉 WebSocket 連線。

## ResultEvent JSON 格式

```json
{
    "schemaVersion": "1.0",
    "eventId": "evt-001",
    "historyId": "hist-001",
    "status": 1000,
    "message": "處理完成",
    "completedAt": 1736668800000,
    "traceId": "trace-001"
}
```

## 腳本說明

| 腳本 | 說明 |
|------|------|
| `e2e-test.sh` | **自動化 E2E 測試** (推薦) |
| `test-notification.sh` | 主要測試腳本，包含所有功能 |
| `kafka-send.sh` | 單純發送 Kafka 訊息 |
| `ws-listen.sh` | 單純監聯 WebSocket |
