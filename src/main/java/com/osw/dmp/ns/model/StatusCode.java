package com.osw.dmp.ns.model;

import lombok.extern.slf4j.Slf4j;

/**
 * 業務狀態碼定義
 * 
 * 1xxx = 成功
 * 2xxx = 業務錯誤（可預期）
 * 3xxx = 系統錯誤（非預期）
 */
@Slf4j
public final class StatusCode {

    private StatusCode() {
    }

    // ==================== 1xxx 成功 ====================

    /** 處理完成 */
    public static final int SUCCESS = 1000;

    /** 查詢成功 */
    public static final int QUERY_SUCCESS = 1001;

    /** 查詢成功（無資料） */
    public static final int QUERY_NO_DATA = 1002;

    // ==================== 2xxx 業務錯誤 ====================

    /** 業務錯誤（通用） */
    public static final int BIZ_ERROR = 2000;

    /** 資料不存在 */
    public static final int NOT_FOUND = 2001;

    /** 權限不足 */
    public static final int FORBIDDEN = 2002;

    /** 資料驗證失敗 */
    public static final int VALIDATION_ERROR = 2003;

    /** 重複請求 */
    public static final int DUPLICATE = 2004;

    // ==================== 3xxx 系統錯誤 ====================

    /** 系統錯誤（通用） */
    public static final int SYS_ERROR = 3000;

    /** 處理超時 */
    public static final int TIMEOUT = 3001;

    /** 服務不可用 */
    public static final int SERVICE_UNAVAILABLE = 3002;

    /** 資源不足 */
    public static final int RESOURCE_EXHAUSTED = 3003;

    // ==================== 輔助方法 ====================

    /**
     * 是否成功
     */
    public static boolean isSuccess(int status) {
        return status >= 1000 && status < 2000;
    }

    /**
     * 是否業務錯誤
     */
    public static boolean isBizError(int status) {
        return status >= 2000 && status < 3000;
    }

    /**
     * 是否系統錯誤
     */
    public static boolean isSysError(int status) {
        return status >= 3000 && status < 4000;
    }

    /**
     * 是否為終態（任務已完成，不論成功或失敗）
     * 終態狀態下應關閉 WebSocket 連線
     */
    public static boolean isTerminal(int status) {
        // 1xxx (成功), 2xxx (業務錯誤), 3xxx (系統錯誤) 都是終態
        // 只有 0xxx 或其他特殊狀態才是中間狀態（如進度更新）
        return status >= 1000;
    }

    /**
     * 根據 status 記錄適當等級的日誌
     */
    public static void logByStatus(org.slf4j.Logger logger, int status, String eventId, String message) {
        if (isSuccess(status)) {
            logger.info("eventId={}, status={}, message={}", eventId, status, message);
        } else if (isBizError(status)) {
            logger.warn("eventId={}, status={}, message={}", eventId, status, message);
        } else if (isSysError(status)) {
            logger.error("eventId={}, status={}, message={}", eventId, status, message);
        } else {
            logger.debug("eventId={}, status={}, message={}", eventId, status, message);
        }
    }
}
