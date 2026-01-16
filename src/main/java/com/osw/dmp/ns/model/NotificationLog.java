package com.osw.dmp.ns.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Notification Log for audit and late-subscription push
 * 
 * Key: {prefix}:{eventId}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationLog implements Serializable {

    /**
     * 事件識別碼
     */
    private String eventId;

    /**
     * 推送狀態
     */
    private PushStatus pushStatus;

    /**
     * 結果載荷（用於補推）
     */
    private ResultPayload resultPayload;

    /**
     * 嘗試次數
     */
    private int attempts;

    /**
     * 建立時間 (epoch millis)
     */
    private Long createdAt;

    /**
     * 更新時間 (epoch millis)
     */
    private Long updatedAt;

    /**
     * 推送狀態列舉
     */
    public enum PushStatus {
        PENDING, // 結果已到達，等待推送
        SENT, // 已成功推送
        FAILED // 重試耗盡仍失敗
    }

    /**
     * 生成快取 Key
     * 格式: {prefix}:{eventId}
     */
    public static String generateKey(String keyPrefix, String eventId) {
        return keyPrefix + ":" + eventId;
    }
}
