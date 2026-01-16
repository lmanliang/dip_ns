package com.osw.dmp.ns.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ResultPayload 單元測試
 * 
 * 測試結果載荷模型
 */
@DisplayName("ResultPayload 單元測試")
class ResultPayloadTest {

    @Nested
    @DisplayName("Builder 測試")
    class BuilderTests {

        @Test
        @DisplayName("應正確建構 ResultPayload")
        void shouldBuildResultPayload() {
            // When
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("處理成功")
                    .build();

            // Then
            assertThat(payload.getStatus()).isEqualTo(StatusCode.SUCCESS);
            assertThat(payload.getMessage()).isEqualTo("處理成功");
        }

        @Test
        @DisplayName("應支援 NoArgsConstructor")
        void shouldSupportNoArgsConstructor() {
            // When
            ResultPayload payload = new ResultPayload();
            payload.setStatus(StatusCode.BIZ_ERROR);
            payload.setMessage("資料驗證失敗");

            // Then
            assertThat(payload.getStatus()).isEqualTo(StatusCode.BIZ_ERROR);
            assertThat(payload.getMessage()).isEqualTo("資料驗證失敗");
        }

        @Test
        @DisplayName("應支援 AllArgsConstructor")
        void shouldSupportAllArgsConstructor() {
            // When
            ResultPayload payload = new ResultPayload(StatusCode.SYS_ERROR, "系統錯誤");

            // Then
            assertThat(payload.getStatus()).isEqualTo(StatusCode.SYS_ERROR);
            assertThat(payload.getMessage()).isEqualTo("系統錯誤");
        }
    }

    @Nested
    @DisplayName("Equals 與 HashCode 測試")
    class EqualsHashCodeTests {

        @Test
        @DisplayName("相同內容的 ResultPayload 應相等")
        void shouldBeEqualForSameContent() {
            ResultPayload payload1 = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("OK")
                    .build();

            ResultPayload payload2 = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("OK")
                    .build();

            assertThat(payload1).isEqualTo(payload2);
            assertThat(payload1.hashCode()).isEqualTo(payload2.hashCode());
        }

        @Test
        @DisplayName("不同 status 的 ResultPayload 不應相等")
        void shouldNotBeEqualForDifferentStatus() {
            ResultPayload payload1 = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("OK")
                    .build();

            ResultPayload payload2 = ResultPayload.builder()
                    .status(StatusCode.BIZ_ERROR)
                    .message("OK")
                    .build();

            assertThat(payload1).isNotEqualTo(payload2);
        }

        @Test
        @DisplayName("不同 message 的 ResultPayload 不應相等")
        void shouldNotBeEqualForDifferentMessage() {
            ResultPayload payload1 = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("OK")
                    .build();

            ResultPayload payload2 = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("Success")
                    .build();

            assertThat(payload1).isNotEqualTo(payload2);
        }
    }

    @Nested
    @DisplayName("Serializable 測試")
    class SerializableTests {

        @Test
        @DisplayName("應實作 Serializable 介面")
        void shouldImplementSerializable() {
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("OK")
                    .build();

            assertThat(payload).isInstanceOf(java.io.Serializable.class);
        }
    }

    @Nested
    @DisplayName("各種狀態碼測試")
    class StatusCodeTests {

        @Test
        @DisplayName("應支援成功狀態碼")
        void shouldSupportSuccessStatusCodes() {
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .build();
            assertThat(StatusCode.isSuccess(payload.getStatus())).isTrue();

            payload.setStatus(StatusCode.QUERY_SUCCESS);
            assertThat(StatusCode.isSuccess(payload.getStatus())).isTrue();

            payload.setStatus(StatusCode.QUERY_NO_DATA);
            assertThat(StatusCode.isSuccess(payload.getStatus())).isTrue();
        }

        @Test
        @DisplayName("應支援業務錯誤狀態碼")
        void shouldSupportBizErrorStatusCodes() {
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.BIZ_ERROR)
                    .build();
            assertThat(StatusCode.isBizError(payload.getStatus())).isTrue();

            payload.setStatus(StatusCode.NOT_FOUND);
            assertThat(StatusCode.isBizError(payload.getStatus())).isTrue();

            payload.setStatus(StatusCode.VALIDATION_ERROR);
            assertThat(StatusCode.isBizError(payload.getStatus())).isTrue();
        }

        @Test
        @DisplayName("應支援系統錯誤狀態碼")
        void shouldSupportSysErrorStatusCodes() {
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.SYS_ERROR)
                    .build();
            assertThat(StatusCode.isSysError(payload.getStatus())).isTrue();

            payload.setStatus(StatusCode.TIMEOUT);
            assertThat(StatusCode.isSysError(payload.getStatus())).isTrue();

            payload.setStatus(StatusCode.SERVICE_UNAVAILABLE);
            assertThat(StatusCode.isSysError(payload.getStatus())).isTrue();
        }
    }

    @Nested
    @DisplayName("空值處理測試")
    class NullHandlingTests {

        @Test
        @DisplayName("message 可以為 null")
        void shouldAllowNullMessage() {
            // When
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message(null)
                    .build();

            // Then
            assertThat(payload.getMessage()).isNull();
        }

        @Test
        @DisplayName("message 可以為空字串")
        void shouldAllowEmptyMessage() {
            // When
            ResultPayload payload = ResultPayload.builder()
                    .status(StatusCode.SUCCESS)
                    .message("")
                    .build();

            // Then
            assertThat(payload.getMessage()).isEmpty();
        }
    }
}
