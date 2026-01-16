#!/bin/bash
# =====================================================
# WebSocket Client - 訂閱並監聽通知
# =====================================================
#
# 依賴: websocat
# 安裝: brew install websocat
# =====================================================

set -e

NS_HOST="${NS_HOST:-localhost}"
NS_PORT="${NS_PORT:-8080}"
WS_URL="ws://${NS_HOST}:${NS_PORT}/ws/notifications"

# 顏色
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 檢查 websocat
if ! command -v websocat &> /dev/null; then
    echo "錯誤: 找不到 websocat"
    echo "請安裝: brew install websocat"
    exit 1
fi

EVENT_ID="$1"

echo -e "${BLUE}[INFO]${NC} 連線到: ${WS_URL}"

if [ -n "$EVENT_ID" ]; then
    echo -e "${BLUE}[INFO]${NC} 訂閱 Event ID: ${EVENT_ID}"
    echo ""
    echo -e "${YELLOW}提示: 收到 'subscribed' 回應後，請在另一個終端機執行:${NC}"
    echo "  ./kafka-send.sh ${EVENT_ID}"
    echo ""
    
    # 建立訂閱訊息
    SUBSCRIBE_MSG="{\"type\":\"subscribe\",\"eventId\":\"${EVENT_ID}\"}"
    
    # 連線並發送訂閱
    (echo "$SUBSCRIBE_MSG"; cat) | websocat "$WS_URL" | while read line; do
        echo -e "${GREEN}[收到]${NC} $line" | jq . 2>/dev/null || echo -e "${GREEN}[收到]${NC} $line"
    done
else
    echo -e "${YELLOW}提示: 連線後手動發送訂閱訊息:${NC}"
    echo '  {"type":"subscribe","eventId":"your-event-id"}'
    echo ""
    echo -e "${BLUE}[INFO]${NC} 監聽中... (Ctrl+C 結束)"
    
    websocat "$WS_URL" | while read line; do
        echo -e "${GREEN}[收到]${NC} $line" | jq . 2>/dev/null || echo -e "${GREEN}[收到]${NC} $line"
    done
fi
