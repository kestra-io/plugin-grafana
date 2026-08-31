package io.kestra.plugin.grafana.loki;

import java.util.Optional;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class TriggerTest {

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
        wireMockServer.stubFor(get(urlPathMatching("/loki/api/v1/query_range.*"))
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
                """)));

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
        wireMockServer.stubFor(get(urlPathMatching("/loki/api/v1/query_range.*"))
            .willReturn(okJson("""
                {
                  "status": "success",
                  "data": {
                    "resultType": "streams",
                    "result": []
                  }
                }
                """)));

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
}
