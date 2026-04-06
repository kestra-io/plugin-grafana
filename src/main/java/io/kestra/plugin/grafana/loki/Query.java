package io.kestra.plugin.grafana.loki;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.grafana.loki.models.LokiQueryResponse;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@Getter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@Schema(
    title = "Run instant LogQL query in Loki",
    description = "Calls the `/loki/api/v1/query` endpoint at a single timestamp. Defaults to limit 100 and BACKWARD sort; time defaults to now."
)
@Plugin(
    examples = {
        @Example(
            title = "Query current error rate",
            full = true,
            code = """
                id: query_loki_instant
                namespace: company.team

                tasks:
                  - id: fetch_error_rate
                    type: io.kestra.plugin.grafana.loki.Query
                    url: http://localhost:3100
                    query: 'sum(rate({job="api"} |= "error" [5m]))'
                    limit: 100
                """
        ),
        @Example(
            title = "Query metrics at a specific time",
            full = true,
            code = """
                id: query_historical_metrics
                type: io.kestra.plugin.grafana.loki.Query
                url: https://loki.example.com
                token: "{{ secret('LOKI_TOKEN') }}"
                tenantId: team-a
                query: 'sum(rate({namespace="production"}[10m])) by (level)'
                time: "2024-01-01T12:00:00Z"
                """
        ),
        @Example(
            title = "Query with authentication",
            full = true,
            code = """
                id: query_metrics
                type: io.kestra.plugin.grafana.loki.Query
                url: https://loki.example.com
                token: "{{ secret('LOKI_TOKEN') }}"
                query: 'count_over_time({environment="staging"}[1h])'
                direction: FORWARD
                """
        )
    },
    metrics = {
        @Metric(
            name = "Records",
            type = Counter.TYPE,
            description = "Total number of log entries retrieved from Loki instant query"
        )
    }
)
public class Query extends AbstractLokiConnection implements RunnableTask<Query.Output> {

    @Schema(
        title = "Evaluation time",
        description = "Timestamp for the instant query in nanoseconds or RFC3339; rendered from flow context. Defaults to current time."
    )
    @PluginProperty(group = "advanced")
    private Property<String> time;

    @Override
    public Output run(RunContext runContext) throws Exception {

        var logger = runContext.logger();

        String rQuery = runContext.render(query).as(String.class).orElseThrow();
        String rTime = this.time != null ? runContext.render(time).as(String.class).orElse(null) : null;
        Integer rLimit = this.limit != null ? runContext.render(limit).as(Integer.class).orElse(null) : null;
        Direction rDirection = runContext.render(direction).as(Direction.class).orElse(Direction.BACKWARD);

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("query", rQuery);

        if (rTime != null) {
            queryParams.put("time", rTime);
        }

        queryParams.put("limit", String.valueOf(rLimit));
        queryParams.put("direction", rDirection.name().toLowerCase());

        String baseUrl = buildBaseUrl(runContext);
        String endpoint = baseUrl + "/loki/api/v1/query";
        URI uri = buildUri(endpoint, queryParams);

        logger.debug("Querying Loki instant: {}", uri);

        HttpResponse<String> res = executeGetReq(runContext, uri);

        ObjectMapper objectMapper = new ObjectMapper();
        LokiQueryResponse lokiQueryResponse = objectMapper.readValue(res.getBody(), LokiQueryResponse.class);

        List<Map<String, Object>> logs = lokiQueryResponse.toLogEntries();

        logger.info("Retrieved {} entries from Loki", logs.size());

        runContext.metric(Counter.of("records", logs.size()));

        return Output.builder()
            .logs(logs)
            .resultType(lokiQueryResponse.getData().getResultType())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Query results",
            description = "Entries include timestamp, labels, and either log line (streams) or value (vector/matrix)"
        )
        private final List<Map<String, Object>> logs;

        @Schema(
            title = "Result type",
            description = "Type of result returned by Loki (vector, streams, or matrix)"
        )
        private final String resultType;
    }
}
