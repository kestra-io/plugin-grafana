package io.kestra.plugin.grafana.loki;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class QueryResultTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testEvaluate() throws Exception {
        // Mock response body
        String responseBody = """
            {
              "status": "success",
              "data": {
                "resultType": "streams",
                "result": [
                  {
                    "stream": {
                      "job": "varlogs",
                      "filename": "/var/log/syslog"
                    },
                    "values": [
                      [
                        "1678275600000000000",
                        "First log line"
                      ],
                      [
                        "1678275601000000000",
                        "Second log line"
                      ]
                    ]
                  }
                ]
              }
            }
            """;

        MockQueryResultTrigger trigger = MockQueryResultTrigger.builder()
            .id("unit-test")
            .type(QueryResultTrigger.class.getName())
            .url(Property.ofValue("http://localhost:3100"))
            .query(Property.ofValue("{job=\"varlogs\"}"))
            .mockResponse(responseBody)
            .build();

        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("loki-test")
            .triggerId("unit-test")
            .build();

        // First run - should return all logs
        Optional<Execution> execution = trigger.evaluate(conditionContext, triggerContext);

        assertThat(execution.isPresent(), is(true));
        assertThat(execution.get().getTrigger().getVariables().get("count"), is(2));
        List<Map<String, Object>> logs = (List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("logs");
        assertThat(logs.size(), is(2));

        // Second run - same response, should return empty because of state
        execution = trigger.evaluate(conditionContext, triggerContext);
        assertThat(execution.isPresent(), is(false));
        
        // Third run - new log
        String newResponseBody = """
            {
              "status": "success",
              "data": {
                "resultType": "streams",
                "result": [
                  {
                    "stream": {
                      "job": "varlogs"
                    },
                    "values": [
                      [
                        "1678275602000000000",
                        "Third log line"
                      ]
                    ]
                  }
                ]
              }
            }
            """;
        trigger.setMockResponse(newResponseBody);
        
        execution = trigger.evaluate(conditionContext, triggerContext);
        assertThat(execution.isPresent(), is(true));
        assertThat(execution.get().getTrigger().getVariables().get("count"), is(1));
    }

    @SuperBuilder
    @Getter
    @Setter
    public static class MockQueryResultTrigger extends QueryResultTrigger {
        private String mockResponse;

        @Override
        protected HttpResponse<String> executeGetReq(RunContext runContext, URI uri) {
            HttpResponse<String> response = mock(HttpResponse.class, Answers.RETURNS_DEEP_STUBS);
            when(response.getBody()).thenReturn(mockResponse);
            when(response.getStatus().getCode()).thenReturn(200);
            return response;
        }
    }
}
```
