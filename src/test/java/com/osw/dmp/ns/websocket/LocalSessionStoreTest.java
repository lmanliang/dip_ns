package com.osw.dmp.ns.websocket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;

/**
 * LocalSessionStore 單元測試
 * 
 * 測試本地 WebSocket Session 儲存邏輯:
 * - Session 註冊與移除
 * - 訂閱與取消訂閱
 * - 查詢功能
 * - 連線/訂閱計數
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("LocalSessionStore 單元測試")
class LocalSessionStoreTest {

    private LocalSessionStore sessionStore;

    @Mock
    private WebSocketSession session1;

    @Mock
    private WebSocketSession session2;

    private static final String SESSION_ID_1 = "session-001";
    private static final String SESSION_ID_2 = "session-002";
    private static final String EVENT_ID_1 = "event-001";
    private static final String EVENT_ID_2 = "event-002";
    private static final String EVENT_ID_3 = "event-003";

    @BeforeEach
    void setUp() {
        sessionStore = new LocalSessionStore();

        // 設定 Mock Session
        lenient().when(session1.getId()).thenReturn(SESSION_ID_1);
        lenient().when(session2.getId()).thenReturn(SESSION_ID_2);
    }

    // ==================== Session 註冊測試 ====================

    @Nested
    @DisplayName("Session 註冊")
    class SessionRegistrationTests {

        @Test
        @DisplayName("註冊新 Session 應增加連線數")
        void shouldIncreaseConnectionCountOnRegister() {
            // Given
            lenient().when(session1.isOpen()).thenReturn(true);

            // When
            sessionStore.registerSession(session1);

            // Then
            assertThat(sessionStore.getConnectionCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("註冊多個 Session 應正確計數")
        void shouldCountMultipleSessions() {
            // Given
            lenient().when(session1.isOpen()).thenReturn(true);
            lenient().when(session2.isOpen()).thenReturn(true);

            // When
            sessionStore.registerSession(session1);
            sessionStore.registerSession(session2);

            // Then
            assertThat(sessionStore.getConnectionCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("已關閉的 Session 不應計入連線數")
        void shouldNotCountClosedSessions() {
            // Given
            lenient().when(session1.isOpen()).thenReturn(true);
            lenient().when(session2.isOpen()).thenReturn(false);

            sessionStore.registerSession(session1);
            sessionStore.registerSession(session2);

            // When
            int count = sessionStore.getConnectionCount();

            // Then
            assertThat(count).isEqualTo(1);
        }
    }

    // ==================== 訂閱測試 ====================

    @Nested
    @DisplayName("訂閱功能")
    class SubscriptionTests {

        @BeforeEach
        void registerSessions() {
            lenient().when(session1.isOpen()).thenReturn(true);
            sessionStore.registerSession(session1);
        }

        @Test
        @DisplayName("訂閱 eventId 應建立映射")
        void shouldCreateMappingOnSubscribe() {
            // When
            sessionStore.subscribe(EVENT_ID_1, session1);

            // Then
            Optional<WebSocketSession> result = sessionStore.getSessionByEventId(EVENT_ID_1);
            assertThat(result).isPresent();
            assertThat(result.get().getId()).isEqualTo(SESSION_ID_1);
        }

        @Test
        @DisplayName("同一 Session 訂閱多個 eventId 應正確處理")
        void shouldHandleMultipleSubscriptionsPerSession() {
            // When
            sessionStore.subscribe(EVENT_ID_1, session1);
            sessionStore.subscribe(EVENT_ID_2, session1);
            sessionStore.subscribe(EVENT_ID_3, session1);

            // Then
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_1)).isPresent();
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_2)).isPresent();
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_3)).isPresent();
            assertThat(sessionStore.getSubscriptionCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("不同 Session 訂閱不同 eventId 應正確處理")
        void shouldHandleSubscriptionsFromDifferentSessions() {
            // Given
            lenient().when(session2.isOpen()).thenReturn(true);
            sessionStore.registerSession(session2);

            // When
            sessionStore.subscribe(EVENT_ID_1, session1);
            sessionStore.subscribe(EVENT_ID_2, session2);

            // Then
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_1).get().getId())
                    .isEqualTo(SESSION_ID_1);
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_2).get().getId())
                    .isEqualTo(SESSION_ID_2);
        }

        @Test
        @DisplayName("訂閱數量應正確計算")
        void shouldCalculateSubscriptionCount() {
            // When
            sessionStore.subscribe(EVENT_ID_1, session1);
            sessionStore.subscribe(EVENT_ID_2, session1);

            // Then
            assertThat(sessionStore.getSubscriptionCount()).isEqualTo(2);
        }
    }

    // ==================== 取消訂閱測試 ====================

    @Nested
    @DisplayName("取消訂閱")
    class UnsubscriptionTests {

        @BeforeEach
        void registerAndSubscribe() {
            lenient().when(session1.isOpen()).thenReturn(true);
            sessionStore.registerSession(session1);
            sessionStore.subscribe(EVENT_ID_1, session1);
            sessionStore.subscribe(EVENT_ID_2, session1);
        }

        @Test
        @DisplayName("取消訂閱應移除映射")
        void shouldRemoveMappingOnUnsubscribe() {
            // When
            sessionStore.unsubscribe(EVENT_ID_1, session1);

            // Then
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_1)).isEmpty();
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_2)).isPresent();
        }

        @Test
        @DisplayName("取消訂閱應更新訂閱數量")
        void shouldUpdateSubscriptionCountOnUnsubscribe() {
            // When
            sessionStore.unsubscribe(EVENT_ID_1, session1);

            // Then
            assertThat(sessionStore.getSubscriptionCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("取消不存在的訂閱應無錯誤")
        void shouldHandleUnsubscribeNonExistent() {
            // When & Then - 不應拋出例外
            sessionStore.unsubscribe("non-existent-event", session1);
            assertThat(sessionStore.getSubscriptionCount()).isEqualTo(2);
        }
    }

    // ==================== Session 移除測試 ====================

    @Nested
    @DisplayName("Session 移除")
    class SessionRemovalTests {

        @BeforeEach
        void registerAndSubscribe() {
            lenient().when(session1.isOpen()).thenReturn(true);
            sessionStore.registerSession(session1);
            sessionStore.subscribe(EVENT_ID_1, session1);
            sessionStore.subscribe(EVENT_ID_2, session1);
        }

        @Test
        @DisplayName("移除 Session 應清除所有訂閱")
        void shouldClearAllSubscriptionsOnRemove() {
            // When
            Set<String> removedEventIds = sessionStore.removeSession(session1);

            // Then
            assertThat(removedEventIds).containsExactlyInAnyOrder(EVENT_ID_1, EVENT_ID_2);
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_1)).isEmpty();
            assertThat(sessionStore.getSessionByEventId(EVENT_ID_2)).isEmpty();
        }

        @Test
        @DisplayName("移除 Session 應返回所有被移除的 eventId")
        void shouldReturnRemovedEventIds() {
            // When
            Set<String> removedEventIds = sessionStore.removeSession(session1);

            // Then
            assertThat(removedEventIds).hasSize(2);
            assertThat(removedEventIds).contains(EVENT_ID_1, EVENT_ID_2);
        }

        @Test
        @DisplayName("移除無訂閱的 Session 應返回空集合")
        void shouldReturnEmptySetForSessionWithNoSubscriptions() {
            // Given
            lenient().when(session2.isOpen()).thenReturn(true);
            sessionStore.registerSession(session2);

            // When
            Set<String> removedEventIds = sessionStore.removeSession(session2);

            // Then
            assertThat(removedEventIds).isEmpty();
        }
    }

    // ==================== 查詢測試 ====================

    @Nested
    @DisplayName("查詢功能")
    class QueryTests {

        @BeforeEach
        void registerAndSubscribe() {
            lenient().when(session1.isOpen()).thenReturn(true);
            sessionStore.registerSession(session1);
            sessionStore.subscribe(EVENT_ID_1, session1);
        }

        @Test
        @DisplayName("根據 eventId 查詢應返回正確 Session")
        void shouldFindSessionByEventId() {
            // When
            Optional<WebSocketSession> result = sessionStore.getSessionByEventId(EVENT_ID_1);

            // Then
            assertThat(result).isPresent();
            assertThat(result.get().getId()).isEqualTo(SESSION_ID_1);
        }

        @Test
        @DisplayName("查詢不存在的 eventId 應返回空")
        void shouldReturnEmptyForNonExistentEventId() {
            // When
            Optional<WebSocketSession> result = sessionStore.getSessionByEventId("non-existent");

            // Then
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("已關閉的 Session 不應返回")
        void shouldNotReturnClosedSession() {
            // Given
            lenient().when(session1.isOpen()).thenReturn(false);

            // When
            Optional<WebSocketSession> result = sessionStore.getSessionByEventId(EVENT_ID_1);

            // Then
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("根據 sessionId 查詢應返回正確 Session")
        void shouldFindSessionBySessionId() {
            // When
            Optional<WebSocketSession> result = sessionStore.getSession(SESSION_ID_1);

            // Then
            assertThat(result).isPresent();
            assertThat(result.get().getId()).isEqualTo(SESSION_ID_1);
        }

        @Test
        @DisplayName("檢查訂閱存在應正確返回")
        void shouldCheckSubscriptionExists() {
            // Then
            assertThat(sessionStore.hasSubscription(EVENT_ID_1)).isTrue();
            assertThat(sessionStore.hasSubscription("non-existent")).isFalse();
        }

        @Test
        @DisplayName("已關閉 Session 的訂閱應視為不存在")
        void shouldNotHaveSubscriptionForClosedSession() {
            // Given
            lenient().when(session1.isOpen()).thenReturn(false);

            // Then
            assertThat(sessionStore.hasSubscription(EVENT_ID_1)).isFalse();
        }

        @Test
        @DisplayName("根據 sessionId 查詢 eventIds 應返回正確結果")
        void shouldGetEventIdsBySession() {
            // Given
            sessionStore.subscribe(EVENT_ID_2, session1);

            // When
            Set<String> eventIds = sessionStore.getEventIdsBySession(SESSION_ID_1);

            // Then
            assertThat(eventIds).containsExactlyInAnyOrder(EVENT_ID_1, EVENT_ID_2);
        }
    }

    // ==================== 並發測試 ====================

    @Nested
    @DisplayName("並發安全")
    class ConcurrencyTests {

        @Test
        @DisplayName("多執行緒同時訂閱應安全")
        void shouldHandleConcurrentSubscriptions() throws InterruptedException {
            // Given
            lenient().when(session1.isOpen()).thenReturn(true);
            sessionStore.registerSession(session1);

            int threadCount = 100;
            Thread[] threads = new Thread[threadCount];

            // When
            for (int i = 0; i < threadCount; i++) {
                final String eventId = "event-" + i;
                threads[i] = new Thread(() -> sessionStore.subscribe(eventId, session1));
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            // Then
            assertThat(sessionStore.getSubscriptionCount()).isEqualTo(threadCount);
        }

        @Test
        @DisplayName("多執行緒同時訂閱和取消訂閱應安全")
        void shouldHandleConcurrentSubscribeAndUnsubscribe() throws InterruptedException {
            // Given
            lenient().when(session1.isOpen()).thenReturn(true);
            sessionStore.registerSession(session1);

            // 先訂閱一些事件
            for (int i = 0; i < 50; i++) {
                sessionStore.subscribe("event-" + i, session1);
            }

            Thread[] subscribeThreads = new Thread[50];
            Thread[] unsubscribeThreads = new Thread[25];

            // When - 同時訂閱新事件和取消部分訂閱
            for (int i = 0; i < 50; i++) {
                final String eventId = "new-event-" + i;
                subscribeThreads[i] = new Thread(() -> sessionStore.subscribe(eventId, session1));
                subscribeThreads[i].start();
            }

            for (int i = 0; i < 25; i++) {
                final String eventId = "event-" + i;
                unsubscribeThreads[i] = new Thread(() -> sessionStore.unsubscribe(eventId, session1));
                unsubscribeThreads[i].start();
            }

            for (Thread thread : subscribeThreads) {
                thread.join();
            }
            for (Thread thread : unsubscribeThreads) {
                thread.join();
            }

            // Then - 應該有 50 + 50 - 25 = 75 個訂閱
            assertThat(sessionStore.getSubscriptionCount()).isEqualTo(75);
        }
    }
}
