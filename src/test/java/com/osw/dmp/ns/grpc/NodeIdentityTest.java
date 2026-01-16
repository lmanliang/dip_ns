package com.osw.dmp.ns.grpc;

import com.osw.dmp.ns.config.GrpcProperties;
import com.osw.dmp.ns.config.NodeProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * NodeIdentity 單元測試
 * 
 * 測試節點身份管理服務
 */
@DisplayName("NodeIdentity 單元測試")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NodeIdentityTest {

    @Mock
    private NodeProperties nodeProperties;

    @Mock
    private GrpcProperties grpcProperties;

    private NodeIdentity nodeIdentity;

    @BeforeEach
    void setUp() {
        when(grpcProperties.getPort()).thenReturn(9090);
    }

    @Nested
    @DisplayName("初始化測試")
    class InitTests {

        @Test
        @DisplayName("使用配置的 nodeId 時應正確初始化")
        void shouldInitWithConfiguredNodeId() {
            // Given
            when(nodeProperties.getId()).thenReturn("configured-node-001");

            nodeIdentity = new NodeIdentity(nodeProperties, grpcProperties);

            // When
            nodeIdentity.init();

            // Then
            assertThat(nodeIdentity.getNodeId()).isEqualTo("configured-node-001");
            assertThat(nodeIdentity.getStartedAtMillis()).isGreaterThan(0);
        }

        @Test
        @DisplayName("未配置 nodeId 時應自動生成")
        void shouldAutoGenerateNodeIdWhenNotConfigured() {
            // Given
            when(nodeProperties.getId()).thenReturn(null);

            nodeIdentity = new NodeIdentity(nodeProperties, grpcProperties);

            // When
            nodeIdentity.init();

            // Then
            assertThat(nodeIdentity.getNodeId()).isNotBlank();
            assertThat(nodeIdentity.getNodeId()).contains("-9090-");
        }

        @Test
        @DisplayName("空白 nodeId 配置時應自動生成")
        void shouldAutoGenerateNodeIdWhenBlank() {
            // Given
            when(nodeProperties.getId()).thenReturn("   ");

            nodeIdentity = new NodeIdentity(nodeProperties, grpcProperties);

            // When
            nodeIdentity.init();

            // Then
            assertThat(nodeIdentity.getNodeId()).isNotBlank();
            assertThat(nodeIdentity.getNodeId()).doesNotContain("   ");
        }

        @Test
        @DisplayName("gRPC 地址應包含正確的端口")
        void shouldGenerateCorrectGrpcAddress() {
            // Given
            when(nodeProperties.getId()).thenReturn("test-node");

            nodeIdentity = new NodeIdentity(nodeProperties, grpcProperties);

            // When
            nodeIdentity.init();

            // Then
            assertThat(nodeIdentity.getGrpcAddress()).endsWith(":9090");
        }

        @Test
        @DisplayName("啟動時間應在初始化時設定")
        void shouldSetStartedAtMillisOnInit() {
            // Given
            when(nodeProperties.getId()).thenReturn("test-node");

            nodeIdentity = new NodeIdentity(nodeProperties, grpcProperties);

            // When
            long beforeInit = System.currentTimeMillis();
            nodeIdentity.init();
            long afterInit = System.currentTimeMillis();

            // Then
            assertThat(nodeIdentity.getStartedAtMillis())
                    .isGreaterThanOrEqualTo(beforeInit)
                    .isLessThanOrEqualTo(afterInit);
        }
    }

    @Nested
    @DisplayName("isCurrentNode 測試")
    class IsCurrentNodeTests {

        @BeforeEach
        void setUpNodeIdentity() {
            when(nodeProperties.getId()).thenReturn("my-node-001");
            nodeIdentity = new NodeIdentity(nodeProperties, grpcProperties);
            nodeIdentity.init();
        }

        @Test
        @DisplayName("相同 nodeId 應回傳 true")
        void shouldReturnTrueForSameNodeId() {
            // Then
            assertThat(nodeIdentity.isCurrentNode("my-node-001")).isTrue();
        }

        @Test
        @DisplayName("不同 nodeId 應回傳 false")
        void shouldReturnFalseForDifferentNodeId() {
            // Then
            assertThat(nodeIdentity.isCurrentNode("other-node-002")).isFalse();
        }

        @Test
        @DisplayName("null nodeId 應回傳 false")
        void shouldReturnFalseForNullNodeId() {
            // Then
            assertThat(nodeIdentity.isCurrentNode(null)).isFalse();
        }
    }

    @Nested
    @DisplayName("不同端口配置測試")
    class DifferentPortTests {

        @Test
        @DisplayName("不同端口應正確反映在 gRPC 地址中")
        void shouldReflectDifferentPortInGrpcAddress() {
            // Given
            when(nodeProperties.getId()).thenReturn("test-node");
            when(grpcProperties.getPort()).thenReturn(19090);

            nodeIdentity = new NodeIdentity(nodeProperties, grpcProperties);

            // When
            nodeIdentity.init();

            // Then
            assertThat(nodeIdentity.getGrpcAddress()).endsWith(":19090");
        }
    }
}
