package io.kestra.plugin.grafana.loki;

import java.net.SocketTimeoutException;

import static org.junit.jupiter.api.Assertions.fail;

final class ReadTimeoutAssertions {
    private ReadTimeoutAssertions() {
    }

    /** Fails unless the cause chain contains the SocketTimeoutException raised by the client's read timeout. */
    static void assertCausedByReadTimeout(Throwable thrown) {
        for (var cause = thrown; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketTimeoutException) {
                return;
            }
        }
        fail("Expected a SocketTimeoutException in the cause chain, but got: " + thrown);
    }
}
