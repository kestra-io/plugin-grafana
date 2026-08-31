package io.kestra.plugin.grafana.loki;

import java.util.Map;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class QueryRangeTest {

    @Inject
    private RunContextFactory runContextFactory;

    private WireMockServer wireMockServer;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(0);
        wireMockServer.start();
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @Test
    void shouldParseStreamsResponseWithoutClassCastException() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query_range"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "streams",
                    "result": [
                      {
                        "stream": {"job": "api", "level": "error"},
                        "values": [
                          ["1700000000000000000", "first error line"],
                          ["1700000001000000000", "second error line"]
                        ]
                      }
                    ]
                  }
                }
                """)));

        QueryRange task = QueryRange.builder()
            .id("query-range-" + TestsUtils.randomString())
            .type(QueryRange.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"api\"} |= \"error\""))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        QueryRange.Output output = task.run(runContext);

        assertThat(output.getResultType(), is("streams"));
        assertThat(output.getLogs(), hasSize(2));
        assertThat(output.getLogs().getFirst().get("line"), is("first error line"));
    }

    @Test
    void shouldParseMatrixResponseWithoutClassCastException() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query_range"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "matrix",
                    "result": [
                      {
                        "metric": {"job": "api"},
                        "values": [
                          ["1700000000000000000", "1"],
                          ["1700000001000000000", "2"]
                        ]
                      }
                    ]
                  }
                }
                """)));

        QueryRange task = QueryRange.builder()
            .id("query-range-" + TestsUtils.randomString())
            .type(QueryRange.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("rate({job=\"api\"}[5m])"))
            .step(Property.ofValue("1m"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        QueryRange.Output output = task.run(runContext);

        assertThat(output.getResultType(), is("matrix"));
        assertThat(output.getLogs(), hasSize(2));
        assertThat(output.getLogs().getFirst().get("value"), is("1"));
    }

    @Test
    void shouldParseEmptyResultSetWithoutError() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query_range"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "streams",
                    "result": []
                  }
                }
                """)));

        QueryRange task = QueryRange.builder()
            .id("query-range-" + TestsUtils.randomString())
            .type(QueryRange.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"api\"} |= \"error\""))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        QueryRange.Output output = task.run(runContext);

        assertThat(output.getLogs(), empty());
    }
}
