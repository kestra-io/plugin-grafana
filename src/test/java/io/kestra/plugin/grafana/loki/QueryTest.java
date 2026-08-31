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
class QueryTest {

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
    void shouldParseVectorResponseWithoutClassCastException() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "vector",
                    "result": [
                      {
                        "metric": {"job": "api"},
                        "value": ["1700000000000000000", "42"]
                      }
                    ]
                  }
                }
                """)));

        Query task = Query.builder()
            .id("query-" + TestsUtils.randomString())
            .type(Query.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("sum(rate({job=\"api\"}[5m]))"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Query.Output output = task.run(runContext);

        assertThat(output.getResultType(), is("vector"));
        assertThat(output.getLogs(), hasSize(1));
        assertThat(output.getLogs().getFirst().get("value"), is("42"));
        assertThat(output.getLogs().getFirst().get("timestamp"), is("1700000000000000000"));
    }

    @Test
    void shouldParseEmptyResultSetWithoutError() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "vector",
                    "result": []
                  }
                }
                """)));

        Query task = Query.builder()
            .id("query-" + TestsUtils.randomString())
            .type(Query.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("sum(rate({job=\"api\"}[5m]))"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Query.Output output = task.run(runContext);

        assertThat(output.getLogs(), empty());
    }
}
