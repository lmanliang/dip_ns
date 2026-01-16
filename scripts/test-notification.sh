#!/bin/bash
# =====================================================
# Notification Service Test Script
# =====================================================
# 測試鏈路: 腳本 → Kafka (results) → NS → WebSocket
#
# 使用方式:
#   ./test-notification.sh [command] [options]
#
# 命令:
#   send      發送測試訊息到 Kafka
#   listen    監聽 WebSocket 通知
#   full      完整測試 (先訂閱，再發送)
#   health    健康檢查
# =====================================================

set -e

# 配置
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-skafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-results}"
NS_HOST="${NS_HOST:-localhost}"
NS_PORT="${NS_PORT:-8080}"
NS_WS_URL="ws://${NS_HOST}:${NS_PORT}/ws/notifications"
NS_API_URL="http://${NS_HOST}:${NS_PORT}/api"

# 顏色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 生成 UUID
generate_uuid() {
    if command -v uuidgen &> /dev/null; then
        uuidgen | tr '[:upper:]' '[:lower:]'
    else
        cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "test-$(date +%s)"
    fi
}

# 生成 ResultEvent JSON
generate_result_event() {
    local event_id="${1:-$(generate_uuid)}"
    local status="${2:-1000}"
    local message="${3:-處理完成}"
    local history_id=$(generate_uuid)
    local timestamp=$(date +%s000)

    cat <<EOF
{
    "schemaVersion": "1.0",
    "eventId": "${event_id}",
    "historyId": "${history_id}",
    "status": ${status},
    "message": "${message}",
    "completedAt": ${timestamp},
    "traceId": "$(generate_uuid)"
}
EOF
}

# 發送訊息到 Kafka
send_to_kafka() {
    local event_id="${1:-$(generate_uuid)}"
    local status="${2:-1000}"
    local message="${3:-處理完成}"
    
    local json=$(generate_result_event "$event_id" "$status" "$message")
    
    log_info "發送訊息到 Kafka topic: ${KAFKA_TOPIC}"
    log_info "Event ID: ${event_id}"
    log_info "Status: ${status}"
    echo ""
    echo "$json" | jq . 2>/dev/null || echo "$json"
    echo ""

    # 使用 kafka-console-producer 或 kcat
    if command -v kcat &> /dev/null; then
        echo "$json" | kcat -b "$KAFKA_BOOTSTRAP" -t "$KAFKA_TOPIC" -P
        log_success "已發送 (kcat)"
    elif command -v kafka-console-producer.sh &> /dev/null; then
        echo "$json" | kafka-console-producer.sh --broker-list "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC"
        log_success "已發送 (kafka-console-producer)"
    elif command -v docker &> /dev/null; then
        echo "$json" | docker exec -i kafka kafka-console-producer.sh --broker-list localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null || \
        echo "$json" | docker exec -i skafka kafka-console-producer.sh --broker-list localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null || \
        log_error "無法透過 Docker 發送"
        log_success "已發送 (docker)"
    else
        log_error "找不到 Kafka 工具 (kcat 或 kafka-console-producer.sh)"
        log_info "請安裝 kcat: brew install kcat"
        return 1
    fi
    
    echo "$event_id"
}

# 健康檢查
check_health() {
    log_info "檢查 Notification Service 健康狀態..."
    
    local health=$(curl -s "${NS_API_URL}/health" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$health" ]; then
        log_success "Health: $(echo $health | jq -r '.status' 2>/dev/null || echo $health)"
    else
        log_error "無法連線到 ${NS_API_URL}/health"
        return 1
    fi

    local ready=$(curl -s "${NS_API_URL}/ready" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$ready" ]; then
        log_success "Ready: $(echo $ready | jq -r '.status' 2>/dev/null || echo $ready)"
        echo "$ready" | jq '.components' 2>/dev/null || true
    fi

    local status=$(curl -s "${NS_API_URL}/status" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$status" ]; then
        log_success "Status:"
        echo "$status" | jq . 2>/dev/null || echo "$status"
    fi
}

# WebSocket 監聽
listen_ws() {
    local event_id="${1:-}"
    
    log_info "連線到 WebSocket: ${NS_WS_URL}"
    
    if ! command -v websocat &> /dev/null; then
        log_error "找不到 websocat，請安裝: brew install websocat"
        return 1
    fi
    
    if [ -n "$event_id" ]; then
        log_info "訂閱 Event ID: ${event_id}"
        echo "{\"type\":\"subscribe\",\"eventId\":\"${event_id}\"}" | websocat -n1 "$NS_WS_URL" &
        sleep 1
    fi
    
    log_info "監聽通知中... (Ctrl+C 結束)"
    websocat "$NS_WS_URL"
}

# 完整測試流程
full_test() {
    local event_id="${1:-$(generate_uuid)}"
    local status="${2:-1000}"
    local message="${3:-測試成功}"
    
    echo "========================================"
    echo "  完整測試流程"
    echo "========================================"
    echo ""
    
    # Step 1: 健康檢查
    log_info "Step 1: 健康檢查"
    check_health || { log_error "服務未就緒"; return 1; }
    echo ""
    
    # Step 2: 提示用戶開啟 WebSocket
    log_info "Step 2: 請在另一個終端機執行以下命令訂閱:"
    echo ""
    echo "  $0 listen ${event_id}"
    echo ""
    echo "  或使用 websocat:"
    echo "  websocat ${NS_WS_URL}"
    echo "  然後發送: {\"type\":\"subscribe\",\"eventId\":\"${event_id}\"}"
    echo ""
    read -p "按 Enter 繼續發送 Kafka 訊息..."
    
    # Step 3: 發送 Kafka 訊息
    log_info "Step 3: 發送 Kafka 訊息"
    send_to_kafka "$event_id" "$status" "$message"
    
    echo ""
    log_success "測試完成！請檢查 WebSocket 終端機是否收到通知"
}

# 快速測試 (使用模擬 API)
quick_test() {
    local event_id="${1:-$(generate_uuid)}"
    local status="${2:-1000}"
    
    log_info "快速測試 (使用模擬 API)"
    log_info "Event ID: ${event_id}"
    
    if [ "$status" -lt 2000 ]; then
        curl -s -X POST "${NS_API_URL}/simulate/success/${event_id}" | jq .
    else
        curl -s -X POST "${NS_API_URL}/simulate/failed/${event_id}?status=${status}" | jq .
    fi
}

# 顯示使用說明
show_help() {
    cat <<EOF
Notification Service 測試腳本

使用方式:
  $0 [command] [options]

命令:
  send [eventId] [status] [message]   發送測試訊息到 Kafka
  listen [eventId]                    監聽 WebSocket (可選擇性訂閱)
  full [eventId] [status] [message]   完整測試流程
  quick [eventId] [status]            快速測試 (使用模擬 API)
  health                              健康檢查
  help                                顯示此說明

環境變數:
  KAFKA_BOOTSTRAP   Kafka 位址 (預設: skafka:9092)
  KAFKA_TOPIC       Kafka Topic (預設: results)
  NS_HOST           NS 主機 (預設: localhost)
  NS_PORT           NS 端口 (預設: 8080)

範例:
  # 健康檢查
  $0 health

  # 發送成功訊息
  $0 send test-001 1000 "處理完成"

  # 發送失敗訊息 (業務錯誤)
  $0 send test-002 2001 "資料不存在"

  # 發送失敗訊息 (系統錯誤)
  $0 send test-003 3001 "處理超時"

  # 完整測試
  $0 full test-004

  # 監聽 WebSocket
  $0 listen

狀態碼說明:
  1000  成功
  2000  業務錯誤
  2001  資料不存在
  2002  驗證失敗
  3000  系統錯誤
  3001  處理超時
  3002  服務不可用
EOF
}

# 主程式
main() {
    local command="${1:-help}"
    shift || true

    case "$command" in
        send)
            send_to_kafka "$@"
            ;;
        listen)
            listen_ws "$@"
            ;;
        full)
            full_test "$@"
            ;;
        quick)
            quick_test "$@"
            ;;
        health)
            check_health
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: $command"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
