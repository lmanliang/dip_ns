package com.osw.dmp.ns.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * NodeStatus 單元測試
 * 
 * 測試節點狀態枚舉
 */
@DisplayName("NodeStatus 單元測試")
class NodeStatusTest {

    @Nested
    @DisplayName("枚舉值測試")
    class EnumValueTests {

        @Test
        @DisplayName("應有 ONLINE 狀態")
        void shouldHaveOnlineStatus() {
            assertThat(NodeStatus.ONLINE).isNotNull();
            assertThat(NodeStatus.ONLINE.name()).isEqualTo("ONLINE");
        }

        @Test
        @DisplayName("應有 SUSPECTED 狀態")
        void shouldHaveSuspectedStatus() {
            assertThat(NodeStatus.SUSPECTED).isNotNull();
            assertThat(NodeStatus.SUSPECTED.name()).isEqualTo("SUSPECTED");
        }

        @Test
        @DisplayName("應有 OFFLINE 狀態")
        void shouldHaveOfflineStatus() {
            assertThat(NodeStatus.OFFLINE).isNotNull();
            assertThat(NodeStatus.OFFLINE.name()).isEqualTo("OFFLINE");
        }
    }

    @Nested
    @DisplayName("枚舉操作測試")
    class EnumOperationTests {

        @Test
        @DisplayName("應能透過 valueOf 取得狀態")
        void shouldGetStatusByValueOf() {
            assertThat(NodeStatus.valueOf("ONLINE")).isEqualTo(NodeStatus.ONLINE);
            assertThat(NodeStatus.valueOf("SUSPECTED")).isEqualTo(NodeStatus.SUSPECTED);
            assertThat(NodeStatus.valueOf("OFFLINE")).isEqualTo(NodeStatus.OFFLINE);
        }

        @Test
        @DisplayName("應有正確的 values 數量")
        void shouldHaveCorrectValuesCount() {
            assertThat(NodeStatus.values()).hasSize(3);
        }

        @Test
        @DisplayName("應有正確的 ordinal 順序")
        void shouldHaveCorrectOrdinalOrder() {
            assertThat(NodeStatus.ONLINE.ordinal()).isEqualTo(0);
            assertThat(NodeStatus.SUSPECTED.ordinal()).isEqualTo(1);
            assertThat(NodeStatus.OFFLINE.ordinal()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("與 NodeInfo 整合測試")
    class NodeInfoIntegrationTests {

        @Test
        @DisplayName("NodeInfo 應能正確設定和取得狀態")
        void shouldSetAndGetStatusInNodeInfo() {
            for (NodeStatus status : NodeStatus.values()) {
                NodeInfo nodeInfo = NodeInfo.builder()
                        .nodeId("test-node")
                        .status(status)
                        .build();

                assertThat(nodeInfo.getStatus()).isEqualTo(status);
            }
        }
    }
}
