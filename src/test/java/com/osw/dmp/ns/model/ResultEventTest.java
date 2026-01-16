package com.osw.dmp.ns.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ResultEvent 單元測試
 * 
 * 測試 Kafka 結果事件模型
 */
@DisplayName("ResultEvent 單元測試")
class ResultEventTest {

    @Nested
    @DisplayName("Builder 測試")
    class BuilderTests {

        @Test
        @DisplayName("應正確建構 ResultEvent")
        void shouldBuildResultEvent() {
            // When
            ResultEvent event = ResultEvent.builder()
                    .schemaVersion("1.0")
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message("處理成功")
                    .completedAt(1704067200000L)
                    .traceId("trace-456")
                    .build();

            // Then
            assertThat(event.getSchemaVersion()).isEqualTo("1.0");
            assertThat(event.getEventId()).isEqualTo("event-123");
            assertThat(event.getStatus()).isEqualTo(StatusCode.SUCCESS);
            assertThat(event.getMessage()).isEqualTo("處理成功");
            assertThat(event.getCompletedAt()).isEqualTo(1704067200000L);
            assertThat(event.getTraceId()).isEqualTo("trace-456");
        }

        @Test
        @DisplayName("應支援 NoArgsConstructor")
        void shouldSupportNoArgsConstructor() {
            // When
            ResultEvent event = new ResultEvent();
            event.setEventId("event-123");
            event.setStatus(StatusCode.SUCCESS);

            // Then
            assertThat(event.getEventId()).isEqualTo("event-123");
            assertThat(event.getStatus()).isEqualTo(StatusCode.SUCCESS);
        }
    }

    @Nested
    @DisplayName("isSuccess 方法測試")
    class IsSuccessTests {

        @Test
        @DisplayName("1xxx 狀態應返回 true")
        void shouldReturnTrueForSuccessStatus() {
            ResultEvent event = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .build();

            assertThat(event.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("2xxx 狀態應返回 false")
        void shouldReturnFalseForBizErrorStatus() {
            ResultEvent event = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.BIZ_ERROR)
                    .build();

            assertThat(event.isSuccess()).isFalse();
        }

        @Test
        @DisplayName("3xxx 狀態應返回 false")
        void shouldReturnFalseForSysErrorStatus() {
            ResultEvent event = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.SYS_ERROR)
                    .build();

            assertThat(event.isSuccess()).isFalse();
        }

        @Test
        @DisplayName("QUERY_SUCCESS 應返回 true")
        void shouldReturnTrueForQuerySuccess() {
            ResultEvent event = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.QUERY_SUCCESS)
                    .build();

            assertThat(event.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("QUERY_NO_DATA 應返回 true")
        void shouldReturnTrueForQueryNoData() {
            ResultEvent event = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.QUERY_NO_DATA)
                    .build();

            assertThat(event.isSuccess()).isTrue();
        }
    }

    @Nested
    @DisplayName("Equals 與 HashCode 測試")
    class EqualsHashCodeTests {

        @Test
        @DisplayName("相同內容的 ResultEvent 應相等")
        void shouldBeEqualForSameContent() {
            ResultEvent event1 = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message("OK")
                    .build();

            ResultEvent event2 = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .message("OK")
                    .build();

            assertThat(event1).isEqualTo(event2);
            assertThat(event1.hashCode()).isEqualTo(event2.hashCode());
        }

        @Test
        @DisplayName("不同 eventId 的 ResultEvent 不應相等")
        void shouldNotBeEqualForDifferentEventId() {
            ResultEvent event1 = ResultEvent.builder()
                    .eventId("event-123")
                    .status(StatusCode.SUCCESS)
                    .build();

            ResultEvent event2 = ResultEvent.builder()
                    .eventId("event-456")
                    .status(StatusCode.SUCCESS)
                    .build();

            assertThat(event1).isNotEqualTo(event2);
        }
    }
}
