package com.osw.dmp.ns.model;

/**
 * 節點狀態列舉
 * 用於追蹤叢集中各節點的運作狀態
 */
public enum NodeStatus {
    /**
     * 正常運作
     */
    ONLINE,

    /**
     * 疑似離線（心跳超時，但尚未確認離線）
     */
    SUSPECTED,

    /**
     * 已確認離線
     */
    OFFLINE
}
