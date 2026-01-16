package com.osw.dmp.ns.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Result Payload stored in NotificationLog for late-subscription push
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResultPayload implements Serializable {

    /**
     * 業務狀態碼 (1xxx成功/2xxx業務錯誤/3xxx系統錯誤)
     */
    private int status;

    /**
     * 訊息內容
     */
    private String message;
}
