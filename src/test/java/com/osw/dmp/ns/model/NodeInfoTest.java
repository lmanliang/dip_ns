package com.osw.dmp.ns.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * NodeInfo 單元測試
 * 
 * 測試節點資訊模型
 */
@DisplayName("NodeInfo 單元測試")
class NodeInfoTest {

    private static final String TEST_NODE_ID = "node-001";
    private static final String TEST_GRPC_ADDRESS = "localhost:9090";

    @Nested
    @DisplayName("Builder 測試")
    class BuilderTests {

        @Test
        @DisplayName("應正確建構 NodeInfo")
        void shouldBuildNodeInfo() {
            // Given
            long now = System.currentTimeMillis();

            // When
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .grpcAddress(TEST_GRPC_ADDRESS)
                    .startedAtMillis(now)
                    .lastHeartbeatMillis(now)
                    .status(NodeStatus.ONLINE)
                    .activeConnections(10)
                    .activeSubscriptions(5)
                    .build();

            // Then
            assertThat(nodeInfo.getNodeId()).isEqualTo(TEST_NODE_ID);
            assertThat(nodeInfo.getGrpcAddress()).isEqualTo(TEST_GRPC_ADDRESS);
            assertThat(nodeInfo.getStartedAtMillis()).isEqualTo(now);
            assertThat(nodeInfo.getLastHeartbeatMillis()).isEqualTo(now);
            assertThat(nodeInfo.getStatus()).isEqualTo(NodeStatus.ONLINE);
            assertThat(nodeInfo.getActiveConnections()).isEqualTo(10);
            assertThat(nodeInfo.getActiveSubscriptions()).isEqualTo(5);
        }

        @Test
        @DisplayName("應支援 NoArgsConstructor")
        void shouldSupportNoArgsConstructor() {
            // When
            NodeInfo nodeInfo = new NodeInfo();
            nodeInfo.setNodeId(TEST_NODE_ID);
            nodeInfo.setGrpcAddress(TEST_GRPC_ADDRESS);

            // Then
            assertThat(nodeInfo.getNodeId()).isEqualTo(TEST_NODE_ID);
            assertThat(nodeInfo.getGrpcAddress()).isEqualTo(TEST_GRPC_ADDRESS);
        }

        @Test
        @DisplayName("應支援 AllArgsConstructor")
        void shouldSupportAllArgsConstructor() {
            // Given
            long now = System.currentTimeMillis();

            // When
            NodeInfo nodeInfo = new NodeInfo(
                    TEST_NODE_ID, TEST_GRPC_ADDRESS, now, now,
                    NodeStatus.ONLINE, 10, 5);

            // Then
            assertThat(nodeInfo.getNodeId()).isEqualTo(TEST_NODE_ID);
            assertThat(nodeInfo.getGrpcAddress()).isEqualTo(TEST_GRPC_ADDRESS);
            assertThat(nodeInfo.getStatus()).isEqualTo(NodeStatus.ONLINE);
        }
    }

    @Nested
    @DisplayName("isOnline 測試")
    class IsOnlineTests {

        @Test
        @DisplayName("ONLINE 狀態應回傳 true")
        void shouldReturnTrueForOnlineStatus() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.ONLINE)
                    .build();

            // Then
            assertThat(nodeInfo.isOnline()).isTrue();
        }

        @Test
        @DisplayName("SUSPECTED 狀態應回傳 false")
        void shouldReturnFalseForSuspectedStatus() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.SUSPECTED)
                    .build();

            // Then
            assertThat(nodeInfo.isOnline()).isFalse();
        }

        @Test
        @DisplayName("OFFLINE 狀態應回傳 false")
        void shouldReturnFalseForOfflineStatus() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.OFFLINE)
                    .build();

            // Then
            assertThat(nodeInfo.isOnline()).isFalse();
        }
    }

    @Nested
    @DisplayName("isAvailableForPush 測試")
    class IsAvailableForPushTests {

        @Test
        @DisplayName("ONLINE 狀態應可用於推送")
        void shouldBeAvailableForOnlineStatus() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.ONLINE)
                    .build();

            // Then
            assertThat(nodeInfo.isAvailableForPush()).isTrue();
        }

        @Test
        @DisplayName("SUSPECTED 狀態應可用於推送")
        void shouldBeAvailableForSuspectedStatus() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.SUSPECTED)
                    .build();

            // Then
            assertThat(nodeInfo.isAvailableForPush()).isTrue();
        }

        @Test
        @DisplayName("OFFLINE 狀態應不可用於推送")
        void shouldNotBeAvailableForOfflineStatus() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.OFFLINE)
                    .build();

            // Then
            assertThat(nodeInfo.isAvailableForPush()).isFalse();
        }
    }

    @Nested
    @DisplayName("updateHeartbeat 測試")
    class UpdateHeartbeatTests {

        @Test
        @DisplayName("更新心跳應更新時間戳和狀態")
        void shouldUpdateHeartbeatTimestampAndStatus() {
            // Given
            long oldTime = System.currentTimeMillis() - 10000;
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .lastHeartbeatMillis(oldTime)
                    .status(NodeStatus.SUSPECTED)
                    .build();

            // When
            long beforeUpdate = System.currentTimeMillis();
            nodeInfo.updateHeartbeat();
            long afterUpdate = System.currentTimeMillis();

            // Then
            assertThat(nodeInfo.getLastHeartbeatMillis())
                    .isGreaterThanOrEqualTo(beforeUpdate)
                    .isLessThanOrEqualTo(afterUpdate);
            assertThat(nodeInfo.getStatus()).isEqualTo(NodeStatus.ONLINE);
        }
    }

    @Nested
    @DisplayName("Equals 與 HashCode 測試")
    class EqualsHashCodeTests {

        @Test
        @DisplayName("相同內容的 NodeInfo 應相等")
        void shouldBeEqualForSameContent() {
            // Given
            long now = System.currentTimeMillis();
            NodeInfo nodeInfo1 = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .grpcAddress(TEST_GRPC_ADDRESS)
                    .startedAtMillis(now)
                    .status(NodeStatus.ONLINE)
                    .build();

            NodeInfo nodeInfo2 = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .grpcAddress(TEST_GRPC_ADDRESS)
                    .startedAtMillis(now)
                    .status(NodeStatus.ONLINE)
                    .build();

            // Then
            assertThat(nodeInfo1).isEqualTo(nodeInfo2);
            assertThat(nodeInfo1.hashCode()).isEqualTo(nodeInfo2.hashCode());
        }

        @Test
        @DisplayName("不同 nodeId 的 NodeInfo 應不相等")
        void shouldNotBeEqualForDifferentNodeId() {
            // Given
            NodeInfo nodeInfo1 = NodeInfo.builder()
                    .nodeId("node-001")
                    .status(NodeStatus.ONLINE)
                    .build();

            NodeInfo nodeInfo2 = NodeInfo.builder()
                    .nodeId("node-002")
                    .status(NodeStatus.ONLINE)
                    .build();

            // Then
            assertThat(nodeInfo1).isNotEqualTo(nodeInfo2);
        }
    }

    @Nested
    @DisplayName("Serializable 測試")
    class SerializableTests {

        @Test
        @DisplayName("應能序列化和反序列化")
        void shouldSerializeAndDeserialize() throws Exception {
            // Given
            long now = System.currentTimeMillis();
            NodeInfo original = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .grpcAddress(TEST_GRPC_ADDRESS)
                    .startedAtMillis(now)
                    .lastHeartbeatMillis(now)
                    .status(NodeStatus.ONLINE)
                    .activeConnections(10)
                    .activeSubscriptions(5)
                    .build();

            // When - Serialize
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(original);
            oos.close();

            // When - Deserialize
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            NodeInfo deserialized = (NodeInfo) ois.readObject();
            ois.close();

            // Then
            assertThat(deserialized).isEqualTo(original);
            assertThat(deserialized.getNodeId()).isEqualTo(TEST_NODE_ID);
            assertThat(deserialized.getGrpcAddress()).isEqualTo(TEST_GRPC_ADDRESS);
            assertThat(deserialized.getStatus()).isEqualTo(NodeStatus.ONLINE);
        }
    }

    @Nested
    @DisplayName("狀態轉換測試")
    class StatusTransitionTests {

        @Test
        @DisplayName("應能從 ONLINE 轉換到 SUSPECTED")
        void shouldTransitionFromOnlineToSuspected() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.ONLINE)
                    .build();

            // When
            nodeInfo.setStatus(NodeStatus.SUSPECTED);

            // Then
            assertThat(nodeInfo.getStatus()).isEqualTo(NodeStatus.SUSPECTED);
            assertThat(nodeInfo.isOnline()).isFalse();
            assertThat(nodeInfo.isAvailableForPush()).isTrue();
        }

        @Test
        @DisplayName("應能從 SUSPECTED 轉換到 OFFLINE")
        void shouldTransitionFromSuspectedToOffline() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.SUSPECTED)
                    .build();

            // When
            nodeInfo.setStatus(NodeStatus.OFFLINE);

            // Then
            assertThat(nodeInfo.getStatus()).isEqualTo(NodeStatus.OFFLINE);
            assertThat(nodeInfo.isOnline()).isFalse();
            assertThat(nodeInfo.isAvailableForPush()).isFalse();
        }

        @Test
        @DisplayName("應能從 SUSPECTED 恢復到 ONLINE")
        void shouldTransitionFromSuspectedToOnline() {
            // Given
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(TEST_NODE_ID)
                    .status(NodeStatus.SUSPECTED)
                    .build();

            // When
            nodeInfo.updateHeartbeat();

            // Then
            assertThat(nodeInfo.getStatus()).isEqualTo(NodeStatus.ONLINE);
            assertThat(nodeInfo.isOnline()).isTrue();
        }
    }
}
