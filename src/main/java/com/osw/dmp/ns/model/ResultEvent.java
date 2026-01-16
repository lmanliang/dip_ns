package com.osw.dmp.ns.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Result Event from Kafka
 * 
 * 格式: {"eventId":"xxx", "status":1000, "message":"...",
 * "timestamp":1736668800000}
 * 
 * Status Code:
 * 1xxx = 成功
 * 2xxx = 業務錯誤
 * 3xxx = 系統錯誤
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResultEvent implements Serializable {

    private String schemaVersion;

    /**
     * 事件識別碼
     */
    private String eventId;

    /**
     * 業務狀態碼 (1xxx成功/2xxx業務錯誤/3xxx系統錯誤)
     */
    private int status;

    /**
     * 訊息內容
     */
    private String message;

    /**
     * 完成時間 (epoch millis)
     */
    private Long completedAt;

    /**
     * 追蹤 ID
     */
    private String traceId;

    /**
     * 是否成功
     */
    public boolean isSuccess() {
        return StatusCode.isSuccess(status);
    }
}
