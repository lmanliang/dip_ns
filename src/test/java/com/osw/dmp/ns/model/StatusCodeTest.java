package com.osw.dmp.ns.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * StatusCode 單元測試
 * 
 * 測試業務狀態碼邏輯:
 * - 1xxx = 成功
 * - 2xxx = 業務錯誤
 * - 3xxx = 系統錯誤
 */
@DisplayName("StatusCode 單元測試")
class StatusCodeTest {

    // ==================== 成功狀態測試 ====================

    @Nested
    @DisplayName("成功狀態 (1xxx)")
    class SuccessStatusTests {

        @ParameterizedTest(name = "狀態碼 {0} 應為成功")
        @ValueSource(ints = { 1000, 1001, 1002, 1100, 1500, 1999 })
        @DisplayName("1xxx 範圍應判定為成功")
        void shouldIdentifySuccessStatus(int status) {
            assertThat(StatusCode.isSuccess(status)).isTrue();
            assertThat(StatusCode.isBizError(status)).isFalse();
            assertThat(StatusCode.isSysError(status)).isFalse();
        }

        @Test
        @DisplayName("SUCCESS 常數應為 1000")
        void successConstantShouldBe1000() {
            assertThat(StatusCode.SUCCESS).isEqualTo(1000);
        }

        @Test
        @DisplayName("QUERY_SUCCESS 常數應為 1001")
        void querySuccessConstantShouldBe1001() {
            assertThat(StatusCode.QUERY_SUCCESS).isEqualTo(1001);
        }

        @Test
        @DisplayName("QUERY_NO_DATA 常數應為 1002")
        void queryNoDataConstantShouldBe1002() {
            assertThat(StatusCode.QUERY_NO_DATA).isEqualTo(1002);
        }
    }

    // ==================== 業務錯誤測試 ====================

    @Nested
    @DisplayName("業務錯誤 (2xxx)")
    class BizErrorStatusTests {

        @ParameterizedTest(name = "狀態碼 {0} 應為業務錯誤")
        @ValueSource(ints = { 2000, 2001, 2002, 2003, 2004, 2500, 2999 })
        @DisplayName("2xxx 範圍應判定為業務錯誤")
        void shouldIdentifyBizErrorStatus(int status) {
            assertThat(StatusCode.isBizError(status)).isTrue();
            assertThat(StatusCode.isSuccess(status)).isFalse();
            assertThat(StatusCode.isSysError(status)).isFalse();
        }

        @Test
        @DisplayName("BIZ_ERROR 常數應為 2000")
        void bizErrorConstantShouldBe2000() {
            assertThat(StatusCode.BIZ_ERROR).isEqualTo(2000);
        }

        @Test
        @DisplayName("NOT_FOUND 常數應為 2001")
        void notFoundConstantShouldBe2001() {
            assertThat(StatusCode.NOT_FOUND).isEqualTo(2001);
        }

        @Test
        @DisplayName("VALIDATION_ERROR 常數應為 2003")
        void validationErrorConstantShouldBe2003() {
            assertThat(StatusCode.VALIDATION_ERROR).isEqualTo(2003);
        }
    }

    // ==================== 系統錯誤測試 ====================

    @Nested
    @DisplayName("系統錯誤 (3xxx)")
    class SysErrorStatusTests {

        @ParameterizedTest(name = "狀態碼 {0} 應為系統錯誤")
        @ValueSource(ints = { 3000, 3001, 3002, 3003, 3500, 3999 })
        @DisplayName("3xxx 範圍應判定為系統錯誤")
        void shouldIdentifySysErrorStatus(int status) {
            assertThat(StatusCode.isSysError(status)).isTrue();
            assertThat(StatusCode.isSuccess(status)).isFalse();
            assertThat(StatusCode.isBizError(status)).isFalse();
        }

        @Test
        @DisplayName("SYS_ERROR 常數應為 3000")
        void sysErrorConstantShouldBe3000() {
            assertThat(StatusCode.SYS_ERROR).isEqualTo(3000);
        }

        @Test
        @DisplayName("TIMEOUT 常數應為 3001")
        void timeoutConstantShouldBe3001() {
            assertThat(StatusCode.TIMEOUT).isEqualTo(3001);
        }

        @Test
        @DisplayName("SERVICE_UNAVAILABLE 常數應為 3002")
        void serviceUnavailableConstantShouldBe3002() {
            assertThat(StatusCode.SERVICE_UNAVAILABLE).isEqualTo(3002);
        }
    }

    // ==================== 終態測試 ====================

    @Nested
    @DisplayName("終態判斷")
    class TerminalStatusTests {

        @ParameterizedTest(name = "狀態碼 {0} 應為終態")
        @ValueSource(ints = { 1000, 1001, 2000, 2001, 3000, 3001 })
        @DisplayName("1xxx, 2xxx, 3xxx 都應為終態")
        void shouldIdentifyTerminalStatus(int status) {
            assertThat(StatusCode.isTerminal(status)).isTrue();
        }

        @ParameterizedTest(name = "狀態碼 {0} 不應為終態")
        @ValueSource(ints = { 0, 100, 500, 999 })
        @DisplayName("小於 1000 的狀態碼不應為終態")
        void shouldNotIdentifyNonTerminalStatus(int status) {
            assertThat(StatusCode.isTerminal(status)).isFalse();
        }
    }

    // ==================== 邊界值測試 ====================

    @Nested
    @DisplayName("邊界值")
    class BoundaryTests {

        @ParameterizedTest(name = "狀態碼 {0}: isSuccess={1}, isBizError={2}, isSysError={3}")
        @CsvSource({
                "999, false, false, false",
                "1000, true, false, false",
                "1999, true, false, false",
                "2000, false, true, false",
                "2999, false, true, false",
                "3000, false, false, true",
                "3999, false, false, true",
                "4000, false, false, false"
        })
        @DisplayName("邊界值應正確判斷")
        void shouldHandleBoundaryValues(int status, boolean isSuccess, boolean isBizError, boolean isSysError) {
            assertThat(StatusCode.isSuccess(status)).isEqualTo(isSuccess);
            assertThat(StatusCode.isBizError(status)).isEqualTo(isBizError);
            assertThat(StatusCode.isSysError(status)).isEqualTo(isSysError);
        }

        @Test
        @DisplayName("負數狀態碼不應匹配任何類型")
        void shouldHandleNegativeStatus() {
            assertThat(StatusCode.isSuccess(-1)).isFalse();
            assertThat(StatusCode.isBizError(-1000)).isFalse();
            assertThat(StatusCode.isSysError(-2000)).isFalse();
            assertThat(StatusCode.isTerminal(-100)).isFalse();
        }
    }
}
