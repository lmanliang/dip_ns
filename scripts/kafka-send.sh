#!/bin/bash
# =====================================================
# Kafka Producer - 發送 ResultEvent 到 Kafka
# =====================================================
# 
# 此腳本直接發送 JSON 訊息到 Kafka topic
# 不依賴 NiFi 或其他中間處理程序
#
# 依賴: kcat (推薦) 或 kafka-console-producer
# 安裝: brew install kcat
# =====================================================

set -e

# 配置 (可透過環境變數覆寫)
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-skafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-results}"

# 顏色
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# 檢查參數
if [ $# -lt 1 ]; then
    echo "使用方式: $0 <eventId> [status] [message]"
    echo ""
    echo "參數:"
    echo "  eventId   事件 ID (必填)"
    echo "  status    狀態碼 (預設: 1000)"
    echo "  message   訊息內容 (預設: 處理完成)"
    echo ""
    echo "範例:"
    echo "  $0 evt-001                    # 成功"
    echo "  $0 evt-002 2001 '資料不存在'   # 業務錯誤"
    echo "  $0 evt-003 3001 '處理超時'     # 系統錯誤"
    exit 1
fi

EVENT_ID="$1"
STATUS="${2:-1000}"
MESSAGE="${3:-處理完成}"
HISTORY_ID=$(uuidgen 2>/dev/null | tr '[:upper:]' '[:lower:]' || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "hist-$(date +%s)")
TRACE_ID=$(uuidgen 2>/dev/null | tr '[:upper:]' '[:lower:]' || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "trace-$(date +%s)")
TIMESTAMP=$(date +%s)000

# 建立 JSON (使用 jq 壓縮成單行)
JSON=$(jq -n -c \
    --arg sv "1.0" \
    --arg eid "${EVENT_ID}" \
    --arg hid "${HISTORY_ID}" \
    --argjson st "${STATUS}" \
    --arg msg "${MESSAGE}" \
    --argjson ts "${TIMESTAMP}" \
    --arg tid "${TRACE_ID}" \
    '{
        schemaVersion: $sv,
        eventId: $eid,
        historyId: $hid,
        status: $st,
        message: $msg,
        completedAt: $ts,
        traceId: $tid
    }')

echo -e "${BLUE}[INFO]${NC} 發送到 Kafka"
echo -e "${BLUE}[INFO]${NC} Bootstrap: ${KAFKA_BOOTSTRAP}"
echo -e "${BLUE}[INFO]${NC} Topic: ${KAFKA_TOPIC}"
echo ""
echo "$JSON" | jq . 2>/dev/null || echo "$JSON"
echo ""

# 發送 (使用 echo -n 避免換行符)
if command -v kcat &> /dev/null; then
    echo -n "$JSON" | kcat -b "$KAFKA_BOOTSTRAP" -t "$KAFKA_TOPIC" -P -k "$EVENT_ID"
    echo -e "${GREEN}[OK]${NC} 已發送 (kcat)"
elif command -v kafka-console-producer.sh &> /dev/null; then
    echo "$JSON" | kafka-console-producer.sh \
        --broker-list "$KAFKA_BOOTSTRAP" \
        --topic "$KAFKA_TOPIC" \
        --property "parse.key=true" \
        --property "key.separator=|"
    echo -e "${GREEN}[OK]${NC} 已發送 (kafka-console-producer)"
else
    echo "錯誤: 找不到 Kafka 工具"
    echo "請安裝 kcat: brew install kcat"
    exit 1
fi
