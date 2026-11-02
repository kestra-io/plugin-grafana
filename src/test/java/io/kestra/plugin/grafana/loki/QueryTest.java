package io.kestra.plugin.grafana.loki;

import java.util.Map;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class QueryTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

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

    @Test
    void shouldFailWhenLokiRespondsSlowerThanReadTimeout() {
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "streams",
                    "result": []
                  }
                }
                """).withFixedDelay(3000)));

        var task = Query.builder()
            .id("query-" + TestsUtils.randomString())
            .type(Query.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"api\"}"))
            .readTimeout(Property.ofValue(1))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var thrown = assertThrows(Exception.class, () -> task.run(runContext));

        ReadTimeoutAssertions.assertCausedByReadTimeout(thrown);
    }

    @Test
    void shouldSucceedWhenLokiRespondsWithinReadTimeout() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "streams",
                    "result": []
                  }
                }
                """).withFixedDelay(1000)));

        var task = Query.builder()
            .id("query-" + TestsUtils.randomString())
            .type(Query.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"api\"}"))
            .readTimeout(Property.ofValue(5))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var output = task.run(runContext);

        assertThat(output.getLogs(), empty());
    }

    @Test
    void shouldRejectNonPositiveTimeouts() {
        var readViolation = modelValidator.isValid(
            Query.builder()
                .id("query-" + TestsUtils.randomString())
                .type(Query.class.getName())
                .url(Property.ofValue("http://localhost:3100"))
                .query(Property.ofValue("{job=\"api\"}"))
                .readTimeout(Property.ofValue(0))
                .build()
        );
        var connectViolation = modelValidator.isValid(
            Query.builder()
                .id("query-" + TestsUtils.randomString())
                .type(Query.class.getName())
                .url(Property.ofValue("http://localhost:3100"))
                .query(Property.ofValue("{job=\"api\"}"))
                .connectTimeout(Property.ofValue(-1))
                .build()
        );

        assertThat(readViolation.isPresent(), is(true));
        assertThat(readViolation.get().getMessage(), containsString("readTimeout"));
        assertThat(connectViolation.isPresent(), is(true));
        assertThat(connectViolation.get().getMessage(), containsString("connectTimeout"));
    }

    @Test
    void shouldFailBeforeAnyRequestWhenFlowSetsZeroReadTimeout() {
        var flow = YamlParser.parse(
            """
                id: loki_zero_read_timeout
                namespace: io.kestra.tests
                tasks:
                  - id: query
                    type: io.kestra.plugin.grafana.loki.Query
                    url: "%s"
                    query: '{job="api"}'
                    readTimeout: 0
                """.formatted(wireMockServer.baseUrl()),
            Flow.class
        );
        var task = (Query) flow.getTasks().getFirst();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var thrown = assertThrows(Exception.class, () -> task.run(runContext));

        assertThat(thrown.getMessage(), containsString("readTimeout"));
        wireMockServer.verify(0, anyRequestedFor(anyUrl()));
    }
}
