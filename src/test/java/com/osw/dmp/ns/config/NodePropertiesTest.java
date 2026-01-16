package com.osw.dmp.ns.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * NodeProperties 單元測試
 * 
 * 測試節點設定屬性
 */
@DisplayName("NodeProperties 單元測試")
class NodePropertiesTest {

    @Nested
    @DisplayName("預設值測試")
    class DefaultValueTests {

        @Test
        @DisplayName("id 預設應為 null")
        void shouldHaveIdDefaultNull() {
            // Given
            NodeProperties properties = new NodeProperties();

            // Then
            assertThat(properties.getId()).isNull();
        }

        @Test
        @DisplayName("heartbeat 應有預設值")
        void shouldHaveHeartbeatDefaults() {
            // Given
            NodeProperties properties = new NodeProperties();
            NodeProperties.HeartbeatConfig heartbeat = properties.getHeartbeat();

            // Then
            assertThat(heartbeat).isNotNull();
            assertThat(heartbeat.getIntervalMs()).isEqualTo(10000);
            assertThat(heartbeat.getTimeoutMs()).isEqualTo(30000);
            assertThat(heartbeat.getOfflineMs()).isEqualTo(60000);
        }
    }

    @Nested
    @DisplayName("setter 測試")
    class SetterTests {

        @Test
        @DisplayName("應能設定 id")
        void shouldSetId() {
            // Given
            NodeProperties properties = new NodeProperties();

            // When
            properties.setId("custom-node-id");

            // Then
            assertThat(properties.getId()).isEqualTo("custom-node-id");
        }

        @Test
        @DisplayName("應能設定 heartbeat properties")
        void shouldSetHeartbeatProperties() {
            // Given
            NodeProperties properties = new NodeProperties();
            NodeProperties.HeartbeatConfig heartbeat = properties.getHeartbeat();

            // When
            heartbeat.setIntervalMs(10000);
            heartbeat.setTimeoutMs(20000);
            heartbeat.setOfflineMs(60000);

            // Then
            assertThat(heartbeat.getIntervalMs()).isEqualTo(10000);
            assertThat(heartbeat.getTimeoutMs()).isEqualTo(20000);
            assertThat(heartbeat.getOfflineMs()).isEqualTo(60000);
        }
    }

    @Nested
    @DisplayName("HeartbeatConfig 測試")
    class HeartbeatConfigTests {

        @Test
        @DisplayName("應能獨立建立 HeartbeatConfig")
        void shouldCreateHeartbeatConfigIndependently() {
            // Given & When
            NodeProperties.HeartbeatConfig heartbeat = new NodeProperties.HeartbeatConfig();

            // Then - 驗證預設值
            assertThat(heartbeat.getIntervalMs()).isEqualTo(10000);
            assertThat(heartbeat.getTimeoutMs()).isEqualTo(30000);
            assertThat(heartbeat.getOfflineMs()).isEqualTo(60000);
        }

        @Test
        @DisplayName("timeout 應大於 interval")
        void timeoutShouldBeGreaterThanInterval() {
            // Given
            NodeProperties.HeartbeatConfig heartbeat = new NodeProperties.HeartbeatConfig();

            // Then
            assertThat(heartbeat.getTimeoutMs()).isGreaterThan(heartbeat.getIntervalMs());
        }

        @Test
        @DisplayName("offline 應大於 timeout")
        void offlineShouldBeGreaterThanTimeout() {
            // Given
            NodeProperties.HeartbeatConfig heartbeat = new NodeProperties.HeartbeatConfig();

            // Then
            assertThat(heartbeat.getOfflineMs()).isGreaterThan(heartbeat.getTimeoutMs());
        }
    }

    @Nested
    @DisplayName("配置驗證測試")
    class ConfigurationValidationTests {

        @Test
        @DisplayName("預設心跳配置應符合合理的時間比例")
        void defaultHeartbeatConfigShouldHaveReasonableRatio() {
            // Given
            NodeProperties properties = new NodeProperties();
            NodeProperties.HeartbeatConfig heartbeat = properties.getHeartbeat();

            // Then
            // interval 應該至少是 1 秒
            assertThat(heartbeat.getIntervalMs()).isGreaterThanOrEqualTo(1000);

            // timeout 應該是 interval 的 2 倍或更多
            assertThat(heartbeat.getTimeoutMs()).isGreaterThanOrEqualTo(heartbeat.getIntervalMs() * 2);

            // offline 應該大於 timeout
            assertThat(heartbeat.getOfflineMs()).isGreaterThan(heartbeat.getTimeoutMs());
        }
    }
}
