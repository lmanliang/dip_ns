package com.osw.dmp.ns.service;

import com.osw.dmp.ns.grpc.CrossNodePushService;
import com.osw.dmp.ns.grpc.NodeIdentity;
import com.osw.dmp.ns.ignite.IgniteCacheService;
import com.osw.dmp.ns.model.ResultEvent;
import com.osw.dmp.ns.model.ResultPayload;
import com.osw.dmp.ns.model.StatusCode;
import com.osw.dmp.ns.model.SubscriptionRouting;
import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import com.osw.dmp.ns.websocket.WebSocketMessages.NotificationMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.test.StepVerifier;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * NotificationService 單元測試
 * 
 * 測試核心通知處理邏輯:
 * - 冪等性檢查
 * - 本地推送
 * - 跨節點路由
 * - Pending 標記
 * - 重試邏輯
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("NotificationService 單元測試")
class NotificationServiceTest {

        @Mock
        private IgniteCacheService igniteCache;

        @Mock
        private NotificationWebSocketHandler webSocketHandler;

        @Mock
        private CrossNodePushService crossNodePushService;

        @Mock
        private NodeIdentity nodeIdentity;

        @InjectMocks
        private NotificationService notificationService;

        @Captor
        private ArgumentCaptor<NotificationMessage> notificationCaptor;

        @Captor
        private ArgumentCaptor<ResultPayload> payloadCaptor;

        private static final String TEST_EVENT_ID = "test-event-123";
        private static final String TEST_NODE_ID = "node-001";
        private static final String TEST_SESSION_ID = "session-001";

        @BeforeEach
        void setUp() {
                // 設定預設配置值
                ReflectionTestUtils.setField(notificationService, "maxRetries", 3);
                ReflectionTestUtils.setField(notificationService, "retryDelayMs", 100L);
        }

        // ==================== 測試資料工廠方法 ====================

        private ResultEvent createSuccessEvent() {
                return ResultEvent.builder()
                                .eventId(TEST_EVENT_ID)
                                .status(StatusCode.SUCCESS)
                                .message("處理成功")
                                .completedAt(System.currentTimeMillis())
                                .build();
        }

        private ResultEvent createBizErrorEvent() {
                return ResultEvent.builder()
                                .eventId(TEST_EVENT_ID)
                                .status(StatusCode.BIZ_ERROR)
                                .message("資料驗證失敗")
                                .completedAt(System.currentTimeMillis())
                                .build();
        }

        private ResultEvent createSysErrorEvent() {
                return ResultEvent.builder()
                                .eventId(TEST_EVENT_ID)
                                .status(StatusCode.SYS_ERROR)
                                .message("系統錯誤")
                                .completedAt(System.currentTimeMillis())
                                .build();
        }

        private SubscriptionRouting createRouting(String nodeId) {
                return SubscriptionRouting.builder()
                                .eventId(TEST_EVENT_ID)
                                .nodeId(nodeId)
                                .grpcAddress("localhost:9090")
                                .sessionId(TEST_SESSION_ID)
                                .subscribedAtMillis(System.currentTimeMillis())
                                .build();
        }

        // ==================== 冪等性測試 ====================

        @Nested
        @DisplayName("冪等性檢查")
        class IdempotencyTests {

                @Test
                @DisplayName("已發送過的事件應被跳過")
                void shouldSkipAlreadySentEvent() {
                        // Given
                        ResultEvent event = createSuccessEvent();
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        verify(igniteCache).isAlreadySent(TEST_EVENT_ID);
                        verify(webSocketHandler, never()).pushNotification(any(), any());
                        verify(igniteCache, never()).markSent(any());
                }

                @Test
                @DisplayName("未發送過的事件應進行處理")
                void shouldProcessNewEvent() {
                        // Given
                        ResultEvent event = createSuccessEvent();
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any())).thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        verify(igniteCache).isAlreadySent(TEST_EVENT_ID);
                        verify(webSocketHandler).pushNotification(eq(TEST_EVENT_ID), any());
                }
        }

        // ==================== 本地推送測試 ====================

        @Nested
        @DisplayName("本地推送")
        class LocalPushTests {

                @Test
                @DisplayName("本地推送成功應標記為已發送")
                void shouldMarkSentOnLocalPushSuccess() {
                        // Given
                        ResultEvent event = createSuccessEvent();
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any())).thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        verify(igniteCache).markSent(TEST_EVENT_ID);
                        verify(igniteCache, never()).markPending(any(), any());
                }

                @Test
                @DisplayName("本地推送應建構正確的通知訊息")
                void shouldBuildCorrectNotificationMessage() {
                        // Given
                        ResultEvent event = createSuccessEvent();
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), notificationCaptor.capture()))
                                        .thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        NotificationMessage captured = notificationCaptor.getValue();
                        assertThat(captured.getEventId()).isEqualTo(TEST_EVENT_ID);
                        assertThat(captured.getStatus()).isEqualTo(StatusCode.SUCCESS);
                        assertThat(captured.getMessage()).isEqualTo("處理成功");
                        assertThat(captured.getTimestamp()).isNotNull();
                        assertThat(captured.getType()).isEqualTo("notification");
                }

                @Test
                @DisplayName("業務錯誤事件應正確處理")
                void shouldHandleBizErrorEvent() {
                        // Given
                        ResultEvent event = createBizErrorEvent();
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), notificationCaptor.capture()))
                                        .thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        NotificationMessage captured = notificationCaptor.getValue();
                        assertThat(captured.getStatus()).isEqualTo(StatusCode.BIZ_ERROR);
                        verify(igniteCache).markSent(TEST_EVENT_ID);
                }

                @Test
                @DisplayName("系統錯誤事件應正確處理")
                void shouldHandleSysErrorEvent() {
                        // Given
                        ResultEvent event = createSysErrorEvent();
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), notificationCaptor.capture()))
                                        .thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        NotificationMessage captured = notificationCaptor.getValue();
                        assertThat(captured.getStatus()).isEqualTo(StatusCode.SYS_ERROR);
                        verify(igniteCache).markSent(TEST_EVENT_ID);
                }
        }

        // ==================== 跨節點路由測試 ====================

        @Nested
        @DisplayName("跨節點路由")
        class CrossNodeRoutingTests {

                @Test
                @DisplayName("本地無訂閱但 Ignite 有路由應嘗試跨節點推送")
                void shouldMarkPendingWhenRoutingExistsButLocalPushFails() {
                        // Given
                        ResultEvent event = createSuccessEvent();
                        SubscriptionRouting routing = createRouting("other-node");

                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any())).thenReturn(false);
                        when(igniteCache.getSubscriptionRouting(TEST_EVENT_ID)).thenReturn(Optional.of(routing));
                        when(nodeIdentity.isCurrentNode("other-node")).thenReturn(false);
                        when(crossNodePushService.push(any(), anyInt(), anyString()))
                                        .thenReturn(CrossNodePushService.PushResult.NODE_UNREACHABLE);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        verify(crossNodePushService).push(eq(routing), eq(StatusCode.SUCCESS), anyString());
                        verify(igniteCache).markPending(eq(TEST_EVENT_ID), any());
                        verify(igniteCache, never()).markSent(any());
                }

                @Test
                @DisplayName("路由指向當前節點但無 session 應標記為 Pending")
                void shouldMarkPendingWhenRoutingPointsToThisNodeButNoSession() {
                        // Given
                        ResultEvent event = createSuccessEvent();
                        SubscriptionRouting routing = createRouting(TEST_NODE_ID);

                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any())).thenReturn(false);
                        when(igniteCache.getSubscriptionRouting(TEST_EVENT_ID)).thenReturn(Optional.of(routing));
                        when(nodeIdentity.isCurrentNode(TEST_NODE_ID)).thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        verify(igniteCache).markPending(eq(TEST_EVENT_ID), any());
                        verify(crossNodePushService, never()).push(any(), anyInt(), anyString());
                }
        }

        // ==================== Pending 標記測試 ====================

        @Nested
        @DisplayName("Pending 標記")
        class PendingMarkTests {

                @Test
                @DisplayName("無訂閱時應標記為 Pending")
                void shouldMarkPendingWhenNoSubscription() {
                        // Given
                        ResultEvent event = createSuccessEvent();
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any())).thenReturn(false);
                        when(igniteCache.getSubscriptionRouting(TEST_EVENT_ID)).thenReturn(Optional.empty());

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        verify(igniteCache).markPending(eq(TEST_EVENT_ID), payloadCaptor.capture());

                        ResultPayload captured = payloadCaptor.getValue();
                        assertThat(captured.getStatus()).isEqualTo(StatusCode.SUCCESS);
                        assertThat(captured.getMessage()).isEqualTo("處理成功");
                }

                @Test
                @DisplayName("Pending 標記應包含正確的 ResultPayload")
                void shouldMarkPendingWithCorrectPayload() {
                        // Given
                        ResultEvent event = ResultEvent.builder()
                                        .eventId(TEST_EVENT_ID)
                                        .status(StatusCode.VALIDATION_ERROR)
                                        .message("欄位 name 不可為空")
                                        .completedAt(System.currentTimeMillis())
                                        .build();

                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any())).thenReturn(false);
                        when(igniteCache.getSubscriptionRouting(TEST_EVENT_ID)).thenReturn(Optional.empty());

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        verify(igniteCache).markPending(eq(TEST_EVENT_ID), payloadCaptor.capture());

                        ResultPayload captured = payloadCaptor.getValue();
                        assertThat(captured.getStatus()).isEqualTo(StatusCode.VALIDATION_ERROR);
                        assertThat(captured.getMessage()).isEqualTo("欄位 name 不可為空");
                }
        }

        // ==================== 多事件處理測試 ====================

        @Nested
        @DisplayName("多事件處理")
        class MultipleEventsTests {

                @Test
                @DisplayName("多個不同事件應獨立處理")
                void shouldProcessMultipleEventsIndependently() {
                        // Given
                        String eventId1 = "event-001";
                        String eventId2 = "event-002";

                        ResultEvent event1 = ResultEvent.builder()
                                        .eventId(eventId1)
                                        .status(StatusCode.SUCCESS)
                                        .message("事件1成功")
                                        .build();

                        ResultEvent event2 = ResultEvent.builder()
                                        .eventId(eventId2)
                                        .status(StatusCode.BIZ_ERROR)
                                        .message("事件2失敗")
                                        .build();

                        when(igniteCache.isAlreadySent(eventId1)).thenReturn(false);
                        when(igniteCache.isAlreadySent(eventId2)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(eventId1), any())).thenReturn(true);
                        when(webSocketHandler.pushNotification(eq(eventId2), any())).thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event1))
                                        .verifyComplete();
                        StepVerifier.create(notificationService.processResultEvent(event2))
                                        .verifyComplete();

                        // Then
                        verify(igniteCache).markSent(eventId1);
                        verify(igniteCache).markSent(eventId2);
                }

                @Test
                @DisplayName("同一事件多次處理應只執行一次")
                void shouldProcessSameEventOnlyOnce() {
                        // Given
                        ResultEvent event = createSuccessEvent();

                        // 第一次處理
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), any())).thenReturn(true);

                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // 第二次處理 - 模擬已發送
                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(true);

                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then - markSent 只應被呼叫一次
                        verify(igniteCache, times(1)).markSent(TEST_EVENT_ID);
                }
        }

        // ==================== 邊界條件測試 ====================

        @Nested
        @DisplayName("邊界條件")
        class EdgeCaseTests {

                @Test
                @DisplayName("空訊息應正確處理")
                void shouldHandleEmptyMessage() {
                        // Given
                        ResultEvent event = ResultEvent.builder()
                                        .eventId(TEST_EVENT_ID)
                                        .status(StatusCode.SUCCESS)
                                        .message("")
                                        .build();

                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), notificationCaptor.capture()))
                                        .thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        assertThat(notificationCaptor.getValue().getMessage()).isEmpty();
                }

                @Test
                @DisplayName("null 訊息應正確處理")
                void shouldHandleNullMessage() {
                        // Given
                        ResultEvent event = ResultEvent.builder()
                                        .eventId(TEST_EVENT_ID)
                                        .status(StatusCode.SUCCESS)
                                        .message(null)
                                        .build();

                        when(igniteCache.isAlreadySent(TEST_EVENT_ID)).thenReturn(false);
                        when(webSocketHandler.pushNotification(eq(TEST_EVENT_ID), notificationCaptor.capture()))
                                        .thenReturn(true);

                        // When
                        StepVerifier.create(notificationService.processResultEvent(event))
                                        .verifyComplete();

                        // Then
                        assertThat(notificationCaptor.getValue().getMessage()).isNull();
                }
        }
}
