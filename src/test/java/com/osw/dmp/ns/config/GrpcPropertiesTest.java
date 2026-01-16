package com.osw.dmp.ns.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * GrpcProperties 單元測試
 * 
 * 測試 gRPC 設定屬性
 */
@DisplayName("GrpcProperties 單元測試")
class GrpcPropertiesTest {

    @Nested
    @DisplayName("預設值測試")
    class DefaultValueTests {

        @Test
        @DisplayName("enabled 預設應為 true")
        void shouldHaveEnabledDefaultTrue() {
            // Given
            GrpcProperties properties = new GrpcProperties();

            // Then
            assertThat(properties.isEnabled()).isTrue();
        }

        @Test
        @DisplayName("port 預設應為 9090")
        void shouldHavePortDefault9090() {
            // Given
            GrpcProperties properties = new GrpcProperties();

            // Then
            assertThat(properties.getPort()).isEqualTo(9090);
        }

        @Test
        @DisplayName("client 應有預設值")
        void shouldHaveClientDefaults() {
            // Given
            GrpcProperties properties = new GrpcProperties();
            GrpcProperties.ClientConfig client = properties.getClient();

            // Then
            assertThat(client).isNotNull();
            assertThat(client.getTimeoutMs()).isEqualTo(5000);
            assertThat(client.getRetryCount()).isEqualTo(2);
            assertThat(client.getRetryDelayMs()).isEqualTo(500);
            assertThat(client.getIdleTimeoutMs()).isEqualTo(300000);
        }
    }

    @Nested
    @DisplayName("setter 測試")
    class SetterTests {

        @Test
        @DisplayName("應能設定 enabled")
        void shouldSetEnabled() {
            // Given
            GrpcProperties properties = new GrpcProperties();

            // When
            properties.setEnabled(false);

            // Then
            assertThat(properties.isEnabled()).isFalse();
        }

        @Test
        @DisplayName("應能設定 port")
        void shouldSetPort() {
            // Given
            GrpcProperties properties = new GrpcProperties();

            // When
            properties.setPort(19090);

            // Then
            assertThat(properties.getPort()).isEqualTo(19090);
        }

        @Test
        @DisplayName("應能設定 client properties")
        void shouldSetClientProperties() {
            // Given
            GrpcProperties properties = new GrpcProperties();
            GrpcProperties.ClientConfig client = properties.getClient();

            // When
            client.setTimeoutMs(10000);
            client.setRetryCount(5);
            client.setRetryDelayMs(1000);
            client.setIdleTimeoutMs(120000);

            // Then
            assertThat(client.getTimeoutMs()).isEqualTo(10000);
            assertThat(client.getRetryCount()).isEqualTo(5);
            assertThat(client.getRetryDelayMs()).isEqualTo(1000);
            assertThat(client.getIdleTimeoutMs()).isEqualTo(120000);
        }
    }

    @Nested
    @DisplayName("ClientConfig 測試")
    class ClientConfigTests {

        @Test
        @DisplayName("應能獨立建立 ClientConfig")
        void shouldCreateClientConfigIndependently() {
            // Given & When
            GrpcProperties.ClientConfig client = new GrpcProperties.ClientConfig();

            // Then - 驗證預設值
            assertThat(client.getTimeoutMs()).isEqualTo(5000);
            assertThat(client.getRetryCount()).isEqualTo(2);
            assertThat(client.getRetryDelayMs()).isEqualTo(500);
            assertThat(client.getIdleTimeoutMs()).isEqualTo(300000);
        }
    }
}
