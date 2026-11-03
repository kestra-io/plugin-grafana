package io.kestra.plugin.grafana.loki;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class TriggerTest {

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
    void shouldParseStreamsResponseWithoutClassCastException() throws Exception {
        wireMockServer.stubFor(
            get(urlPathMatching("/loki/api/v1/query_range.*"))
                .willReturn(okJson("""
                    {
                      "status": "success",
                      "data": {
                        "resultType": "streams",
                        "result": [
                          {
                            "stream": {"job": "security", "level": "critical"},
                            "values": [
                              ["1700000000000000000", "unauthorized access attempt"]
                            ]
                          }
                        ]
                      }
                    }
                    """))
        );

        Trigger trigger = Trigger.builder()
            .id("loki-" + IdUtils.create())
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"security\"} |= \"unauthorized access\""))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        assertThat(execution.get().getTrigger().getVariables().get("count"), is(1));
    }

    @Test
    void shouldNotFireOnEmptyResultSet() throws Exception {
        wireMockServer.stubFor(
            get(urlPathMatching("/loki/api/v1/query_range.*"))
                .willReturn(okJson("""
                    {
                      "status": "success",
                      "data": {
                        "resultType": "streams",
                        "result": []
                      }
                    }
                    """))
        );

        Trigger trigger = Trigger.builder()
            .id("loki-" + IdUtils.create())
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"security\"} |= \"unauthorized access\""))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(false));
    }

    @Test
    void shouldFailWhenLokiRespondsSlowerThanReadTimeout() throws Exception {
        wireMockServer.stubFor(
            get(urlPathMatching("/loki/api/v1/query_range.*"))
                .willReturn(okJson("""
                    {
                      "status": "success",
                      "data": {
                        "resultType": "streams",
                        "result": []
                      }
                    }
                    """).withFixedDelay(3000))
        );

        var trigger = Trigger.builder()
            .id("loki-" + IdUtils.create())
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"security\"}"))
            .readTimeout(Property.ofValue(1))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var thrown = assertThrows(Exception.class, () -> trigger.evaluate(context.getKey(), context.getValue()));

        ReadTimeoutAssertions.assertCausedByReadTimeout(thrown);
    }

    @Test
    void shouldRejectNonPositiveReadTimeout() {
        var violation = modelValidator.isValid(
            Trigger.builder()
                .id("loki-" + IdUtils.create())
                .type(Trigger.class.getName())
                .url(Property.ofValue("http://localhost:3100"))
                .query(Property.ofValue("{job=\"security\"}"))
                .readTimeout(Property.ofValue(0))
                .build()
        );

        assertThat(violation.isPresent(), is(true));
        assertThat(violation.get().getMessage(), containsString("readTimeout"));
    }

    @Test
    void shouldPageFromNewestLineWhenWindowHasMoreThanMaxRecords() throws Exception {
        var pageSize = 1500;
        var olderStream = new StringJoiner(",");
        for (var i = 2; i <= pageSize; i++) {
            olderStream.add("[\"%d\", \"line %d\"]".formatted(1700000000000000000L + i, i));
        }
        wireMockServer.stubFor(
            get(urlPathEqualTo("/loki/api/v1/query_range"))
                .withQueryParam("since", equalTo("10m"))
                .willReturn(
                    okJson(
                        streams(
                            "{\"stream\": {\"job\": \"a\"}, \"values\": [%s]}".formatted(olderStream),
                            "{\"stream\": {\"job\": \"b\"}, \"values\": [[\"1700000000000000001\", \"line 1\"]]}"
                        )
                    )
                )
        );
        wireMockServer.stubFor(
            get(urlPathEqualTo("/loki/api/v1/query_range"))
                .withQueryParam("start", equalTo(String.valueOf(1700000000000000000L + pageSize)))
                .willReturn(
                    okJson(
                        streams(
                            "{\"stream\": {\"job\": \"a\"}, \"values\": [[\"%d\", \"line %d\"], [\"%d\", \"line %d\"]]}".formatted(
                                1700000000000000000L + pageSize, pageSize, 1700000000000000000L + pageSize + 1, pageSize + 1
                            )
                        )
                    )
                )
        );

        var trigger = Trigger.builder()
            .id("loki-%s".formatted(IdUtils.create()))
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=~\"a|b\"}"))
            .maxRecords(Property.ofValue(pageSize))
            .build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));
        assertThat(first.get().getTrigger().getVariables().get("count"), is(pageSize));

        var second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(true));
        assertThat(second.get().getTrigger().getVariables().get("count"), is(1));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldFireOldestLinesFirstAcrossStreams() throws Exception {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/loki/api/v1/query_range"))
                .willReturn(
                    okJson(
                        streams(
                            "{\"stream\": {\"job\": \"a\"}, \"values\": [[\"1700000000000000003\", \"a3\"], [\"1700000000000000004\", \"a4\"]]}",
                            "{\"stream\": {\"job\": \"b\"}, \"values\": [[\"1700000000000000001\", \"b1\"]]}"
                        )
                    )
                )
        );

        var trigger = Trigger.builder()
            .id("loki-%s".formatted(IdUtils.create()))
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=~\"a|b\"}"))
            .maxRecords(Property.ofValue(2))
            .build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        var logs = (List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("logs");
        assertThat(logs.stream().map(log -> log.get("line")).toList(), contains("b1", "a3"));
    }

    @Test
    void shouldStepPastTimestampSharedByFullPage() throws Exception {
        var pageSize = 1000;
        var timestamp = 1700000000000000000L;
        var values = new StringJoiner(",");
        for (var i = 0; i < pageSize; i++) {
            values.add("[\"%d\", \"line %d\"]".formatted(timestamp, i));
        }
        var fullPage = okJson(streams("{\"stream\": {\"job\": \"a\"}, \"values\": [%s]}".formatted(values)));
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query_range")).withQueryParam("since", equalTo("10m")).willReturn(fullPage));
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query_range")).withQueryParam("start", equalTo(String.valueOf(timestamp))).willReturn(fullPage));
        wireMockServer.stubFor(get(urlPathEqualTo("/loki/api/v1/query_range")).withQueryParam("start", equalTo(String.valueOf(timestamp + 1))).willReturn(okJson(streams())));

        var trigger = Trigger.builder()
            .id("loki-%s".formatted(IdUtils.create()))
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockServer.baseUrl()))
            .query(Property.ofValue("{job=\"a\"}"))
            .maxRecords(Property.ofValue(pageSize))
            .build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        wireMockServer.verify(getRequestedFor(urlPathEqualTo("/loki/api/v1/query_range")).withQueryParam("start", equalTo(String.valueOf(timestamp + 1))));
    }

    private static String streams(String... streams) {
        return """
            {"status": "success", "data": {"resultType": "streams", "result": [%s]}}
            """.formatted(String.join(",", streams));
    }
}
