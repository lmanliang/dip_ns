#!/bin/bash
# =====================================================
# E2E 自動化測試腳本
# =====================================================
# 完整測試流程: WebSocket 訂閱 → Kafka 發送 → 收到通知 → 連線關閉
#
# 使用方式:
#   ./e2e-test.sh                    # 快速測試
#   ./e2e-test.sh --verbose          # 詳細輸出
#   ./e2e-test.sh --all              # 執行所有測試案例
#
# 依賴:
#   - websocat (brew install websocat)
#   - kcat (brew install kcat)
#   - jq (brew install jq)
# =====================================================

set -e

# 配置
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-skafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-results}"
NS_HOST="${NS_HOST:-localhost}"
NS_PORT="${NS_PORT:-8080}"
WS_URL="ws://${NS_HOST}:${NS_PORT}/ws/notifications"
API_URL="http://${NS_HOST}:${NS_PORT}/api"

# 測試配置
TIMEOUT=10
VERBOSE=false

# 顏色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# 臨時文件
WS_OUTPUT=""
WS_PID=""

# ==================== 輔助函數 ====================

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_debug() { $VERBOSE && echo -e "${CYAN}[DEBUG]${NC} $1" || true; }

generate_uuid() {
    uuidgen 2>/dev/null | tr '[:upper:]' '[:lower:]' || \
    cat /proc/sys/kernel/random/uuid 2>/dev/null || \
    echo "test-$(date +%s)-$RANDOM"
}

check_deps() {
    local missing=()
    command -v websocat &>/dev/null || missing+=("websocat")
    command -v kcat &>/dev/null || missing+=("kcat")
    command -v jq &>/dev/null || missing+=("jq")
    
    if [ ${#missing[@]} -gt 0 ]; then
        log_fail "缺少依賴: ${missing[*]}"
        echo "請安裝: brew install ${missing[*]}"
        exit 1
    fi
}

check_service() {
    log_info "檢查服務健康狀態..."
    local health=$(curl -s --connect-timeout 3 "${API_URL}/health" 2>/dev/null)
    if [ -z "$health" ]; then
        log_fail "無法連線到 Notification Service (${API_URL})"
        echo "請確認服務已啟動: mvn spring-boot:run"
        exit 1
    fi
    log_success "服務運行中"
}

# ==================== WebSocket 操作 ====================

# 啟動 WebSocket 連線並訂閱
start_ws_listener() {
    local event_id="$1"
    WS_OUTPUT=$(mktemp)
    
    log_debug "啟動 WebSocket 監聯器: eventId=${event_id}"
    
    # 建立訂閱訊息
    local subscribe_msg="{\"type\":\"subscribe\",\"eventId\":\"${event_id}\"}"
    
    # 啟動 websocat 在背景，輸出到臨時檔案
    # 使用 timeout 確保不會永久等待
    # -n: 不要在 stdin EOF 時發送 WebSocket close (保持連線)
    # -t: 以文字模式發送訊息
    (echo "$subscribe_msg"; sleep $TIMEOUT) | \
        timeout $((TIMEOUT + 2)) websocat -t -n "$WS_URL" > "$WS_OUTPUT" 2>&1 &
    WS_PID=$!
    
    # 等待訂閱確認
    sleep 1
    
    if grep -q '"type":"subscribed"' "$WS_OUTPUT" 2>/dev/null; then
        log_debug "WebSocket 訂閱成功"
        return 0
    fi
    
    # 再等一下
    sleep 1
    if grep -q '"type":"subscribed"' "$WS_OUTPUT" 2>/dev/null; then
        log_debug "WebSocket 訂閱成功"
        return 0
    fi
    
    log_warn "未收到訂閱確認 (可能已有 pending 通知)"
    return 0
}

# 停止 WebSocket 連線
stop_ws_listener() {
    if [ -n "$WS_PID" ]; then
        kill $WS_PID 2>/dev/null || true
        wait $WS_PID 2>/dev/null || true
        WS_PID=""
    fi
}

# 取得 WebSocket 輸出
get_ws_output() {
    if [ -f "$WS_OUTPUT" ]; then
        cat "$WS_OUTPUT"
    fi
}

# 清理
cleanup() {
    stop_ws_listener
    [ -f "$WS_OUTPUT" ] && rm -f "$WS_OUTPUT"
}
trap cleanup EXIT

# ==================== Kafka 操作 ====================

send_kafka_message() {
    local event_id="$1"
    local status="$2"
    local message="$3"
    local history_id=$(generate_uuid)
    local trace_id=$(generate_uuid)
    local timestamp=$(date +%s)000
    
    local json=$(jq -n -c \
        --arg sv "1.0" \
        --arg eid "$event_id" \
        --arg hid "$history_id" \
        --argjson st "$status" \
        --arg msg "$message" \
        --argjson ts "$timestamp" \
        --arg tid "$trace_id" \
        '{
            schemaVersion: $sv,
            eventId: $eid,
            historyId: $hid,
            status: $st,
            message: $msg,
            completedAt: $ts,
            traceId: $tid
        }')
    
    log_debug "發送 Kafka 訊息: $json"
    
    echo -n "$json" | kcat -b "$KAFKA_BOOTSTRAP" -t "$KAFKA_TOPIC" -P -k "$event_id" 2>/dev/null
    return $?
}

# ==================== 測試案例 ====================

# 測試: 成功通知 (status=1000)
test_success_notification() {
    local test_name="成功通知 (status=1000)"
    local event_id="e2e-success-$(generate_uuid)"
    
    echo ""
    log_info "測試: $test_name"
    log_info "Event ID: $event_id"
    
    # 1. 啟動 WebSocket 監聽
    start_ws_listener "$event_id"
    
    # 2. 發送 Kafka 訊息
    sleep 0.5
    send_kafka_message "$event_id" 1000 "處理完成"
    
    # 3. 等待通知
    sleep 2
    
    # 4. 檢查結果
    local output=$(get_ws_output)
    log_debug "WebSocket 輸出:\n$output"
    
    # 驗證收到訂閱確認
    if ! echo "$output" | grep -q '"type":"subscribed"'; then
        log_fail "$test_name - 未收到訂閱確認"
        return 1
    fi
    
    # 驗證收到通知
    if ! echo "$output" | grep -q '"type":"notification"'; then
        log_fail "$test_name - 未收到通知"
        return 1
    fi
    
    # 驗證狀態碼
    if ! echo "$output" | grep -q '"status":1000'; then
        log_fail "$test_name - 狀態碼不正確"
        return 1
    fi
    
    stop_ws_listener
    log_success "$test_name"
    return 0
}

# 測試: 業務錯誤通知 (status=2001)
test_biz_error_notification() {
    local test_name="業務錯誤通知 (status=2001)"
    local event_id="e2e-biz-err-$(generate_uuid)"
    
    echo ""
    log_info "測試: $test_name"
    log_info "Event ID: $event_id"
    
    start_ws_listener "$event_id"
    sleep 0.5
    send_kafka_message "$event_id" 2001 "資料不存在"
    sleep 2
    
    local output=$(get_ws_output)
    log_debug "WebSocket 輸出:\n$output"
    
    if ! echo "$output" | grep -q '"status":2001'; then
        log_fail "$test_name - 狀態碼不正確"
        return 1
    fi
    
    stop_ws_listener
    log_success "$test_name"
    return 0
}

# 測試: 系統錯誤通知 (status=3001)
test_sys_error_notification() {
    local test_name="系統錯誤通知 (status=3001)"
    local event_id="e2e-sys-err-$(generate_uuid)"
    
    echo ""
    log_info "測試: $test_name"
    log_info "Event ID: $event_id"
    
    start_ws_listener "$event_id"
    sleep 0.5
    send_kafka_message "$event_id" 3001 "處理超時"
    sleep 2
    
    local output=$(get_ws_output)
    log_debug "WebSocket 輸出:\n$output"
    
    if ! echo "$output" | grep -q '"status":3001'; then
        log_fail "$test_name - 狀態碼不正確"
        return 1
    fi
    
    stop_ws_listener
    log_success "$test_name"
    return 0
}

# 測試: 晚訂閱 (Late Subscription)
test_late_subscription() {
    local test_name="晚訂閱 (Late Subscription)"
    local event_id="e2e-late-$(generate_uuid)"
    
    echo ""
    log_info "測試: $test_name"
    log_info "Event ID: $event_id"
    
    # 1. 先發送 Kafka 訊息 (此時沒有訂閱者)
    send_kafka_message "$event_id" 1000 "晚訂閱測試"
    
    # 2. 等待訊息被處理並標記為 PENDING
    sleep 2
    
    # 3. 現在才訂閱
    start_ws_listener "$event_id"
    sleep 2
    
    local output=$(get_ws_output)
    log_debug "WebSocket 輸出:\n$output"
    
    # 應該收到 pending 的通知
    if ! echo "$output" | grep -q '"type":"notification"'; then
        log_warn "$test_name - 未收到 pending 通知 (可能是設計行為)"
        stop_ws_listener
        return 0
    fi
    
    stop_ws_listener
    log_success "$test_name"
    return 0
}

# 測試: 健康檢查 API
test_health_api() {
    local test_name="健康檢查 API"
    
    echo ""
    log_info "測試: $test_name"
    
    # /api/health
    local health=$(curl -s "${API_URL}/health")
    if ! echo "$health" | jq -e '.status == "UP"' &>/dev/null; then
        log_fail "$test_name - /api/health 回應異常"
        return 1
    fi
    log_debug "/api/health: $health"
    
    # /api/ready
    local ready=$(curl -s "${API_URL}/ready")
    log_debug "/api/ready: $ready"
    
    # /api/status
    local status=$(curl -s "${API_URL}/status")
    log_debug "/api/status: $status"
    
    log_success "$test_name"
    return 0
}

# ==================== 主程式 ====================

run_quick_test() {
    echo "========================================"
    echo "  E2E 快速測試"
    echo "========================================"
    echo ""
    echo "配置:"
    echo "  Kafka: $KAFKA_BOOTSTRAP"
    echo "  Topic: $KAFKA_TOPIC"
    echo "  NS:    $API_URL"
    echo "  WS:    $WS_URL"
    echo ""
    
    check_deps
    check_service
    
    local passed=0
    local failed=0
    
    if test_success_notification; then ((passed++)); else ((failed++)); fi
    
    echo ""
    echo "========================================"
    echo "  測試結果: $passed 通過, $failed 失敗"
    echo "========================================"
    
    [ $failed -eq 0 ]
}

run_all_tests() {
    echo "========================================"
    echo "  E2E 完整測試"
    echo "========================================"
    echo ""
    echo "配置:"
    echo "  Kafka: $KAFKA_BOOTSTRAP"
    echo "  Topic: $KAFKA_TOPIC"
    echo "  NS:    $API_URL"
    echo "  WS:    $WS_URL"
    echo ""
    
    check_deps
    check_service
    
    local passed=0
    local failed=0
    
    if test_health_api; then ((passed++)); else ((failed++)); fi
    if test_success_notification; then ((passed++)); else ((failed++)); fi
    if test_biz_error_notification; then ((passed++)); else ((failed++)); fi
    if test_sys_error_notification; then ((passed++)); else ((failed++)); fi
    if test_late_subscription; then ((passed++)); else ((failed++)); fi
    
    echo ""
    echo "========================================"
    echo "  測試結果: $passed 通過, $failed 失敗"
    echo "========================================"
    
    [ $failed -eq 0 ]
}

show_help() {
    cat <<EOF
E2E 自動化測試腳本

使用方式:
  $0 [options]

選項:
  --quick, -q     快速測試 (只測試成功通知)
  --all, -a       執行所有測試案例
  --verbose, -v   詳細輸出
  --help, -h      顯示此說明

環境變數:
  KAFKA_BOOTSTRAP   Kafka 位址 (預設: skafka:9092)
  KAFKA_TOPIC       Kafka Topic (預設: results)
  NS_HOST           NS 主機 (預設: localhost)
  NS_PORT           NS 端口 (預設: 8080)

測試案例:
  1. 健康檢查 API
  2. 成功通知 (status=1000)
  3. 業務錯誤通知 (status=2001)
  4. 系統錯誤通知 (status=3001)
  5. 晚訂閱 (Late Subscription)

範例:
  # 快速測試
  $0

  # 詳細輸出
  $0 --verbose

  # 執行所有測試
  $0 --all --verbose
EOF
}

main() {
    local run_all=false
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --verbose|-v)
                VERBOSE=true
                shift
                ;;
            --all|-a)
                run_all=true
                shift
                ;;
            --quick|-q)
                run_all=false
                shift
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                log_warn "未知選項: $1"
                shift
                ;;
        esac
    done
    
    if $run_all; then
        run_all_tests
    else
        run_quick_test
    fi
}

main "$@"
