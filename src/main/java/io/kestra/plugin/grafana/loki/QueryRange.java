package io.kestra.plugin.grafana.loki;

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
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@Getter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@Schema(
    title = "Query logs from Grafana Loki within a time range",
    description = "Execute LogQL queries against Loki over a specified time range. Supports both log queries (returning stream responses) and metric queries (returning matrix responses)."
)
@Plugin(
    examples = {
        @Example(
            title = "Query error logs from the last hour using relative time",
            full = true,
            code = """
                id: query_loki_errors
                namespace: company.team

                tasks:
                  - id: fetch_errors
                    type: io.kestra.plugin.grafana.loki.QueryRange
                    url: http://localhost:3100
                    query: '{job="api"} |= "error"'
                    since: 1h
                    limit: 1000
                """
        ),
        @Example(
            title = "Query with authentication and absolute time range",
            full = true,
            code = """
                id: query_loki
                namespace: company.team

                tasks:
                  - id: fetch_logs
                    type: io.kestra.plugin.grafana.loki.QueryRange
                    url: https://loki.example.com
                    token: "{{ secret('LOKI_TOKEN') }}"
                    tenantId: team-a
                    query: '{namespace="production", container="frontend"}'
                    start: "2024-01-01T00:00:00Z"
                    end: "2024-01-01T23:59:59Z"
                    limit: 5000
                    direction: BACKWARD
                """
        ),
        @Example(
            title = "Metric query with step interval",
            full = true,
            code = """
                id: loki_test
                namespace: company.team

                tasks:
                  - id: query_metrics
                    type: io.kestra.plugin.grafana.loki.QueryRange
                    url: http://localhost:3100
                    query: 'rate({job="api"}[5m])'
                    start: "{{ now() | dateAdd(-6, 'HOURS') }}"
                    step: 1m
                """
        )
    },
    metrics = {
        @Metric(
            name = "Records",
            type = Counter.TYPE,
            description = "Total number of log entries retrieved from Loki range query"
        )
    }
)
public class QueryRange extends AbstractLokiConnection implements RunnableTask<QueryRange.Output> {

    @Schema(
        title = "Start time",
        description = "The start time for the query as a nanosecond Unix epoch or another supported format (e.g., RFC3339). Defaults to one hour ago."
    )
    private Property<String> start;

    @Schema(
        title = "End time",
        description = "The end time for the query as a nanosecond Unix epoch or another supported format (e.g., RFC3339). Defaults to now."
    )
    private Property<String> end;

    @Schema(
        title = "Step",
        description = "Query resolution step width in duration format (e.g., '1m', '30s') or seconds. Applies only to metric queries that return matrix responses."
    )
    private Property<String> step;

    @Schema(
        title = "Since",
        description = "Duration to calculate start time relative to end time (e.g., '1h', '30m'). Alternative to specifying absolute start time."
    )
    private Property<String> since;

    @Schema(
        title = "Interval",
        description = "Entries are returned at the specified interval. Only applies to log queries that return stream responses (e.g., '1m', '30s')."
    )
    private Property<String> interval;

    @Override
    public Output run(RunContext runContext) throws Exception {

        var logger = runContext.logger();

        String rQuery = runContext.render(query).as(String.class).orElseThrow();
        String rStart = this.start != null ? runContext.render(start).as(String.class).orElse(null) : null;
        String rEnd = this.end != null ? runContext.render(end).as(String.class).orElse(null) : null;
        Integer rLimit = this.limit != null ? runContext.render(limit).as(Integer.class).orElse(null) : null;
        String rStep = this.step != null ? runContext.render(step).as(String.class).orElse(null) : null;
        String rSince = this.since != null ? runContext.render(since).as(String.class).orElse(null) : null;
        String rInterval = this.interval != null ? runContext.render(interval).as(String.class).orElse(null) : null;
        Direction rDirection = runContext.render(direction).as(Direction.class).orElse(Direction.BACKWARD);

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("query",rQuery);

        if (rStart != null) {
            queryParams.put("start", rStart);
        }

        if (rEnd != null) {
            queryParams.put("end", rEnd);
        }

        if (rSince != null) {
            queryParams.put("since", rSince);
        }

        queryParams.put("limit", String.valueOf(rLimit));
        queryParams.put("direction", rDirection.name().toLowerCase());

        if (rStep != null) {
            queryParams.put("step", rStep);
        }

        if (rInterval != null) {
            queryParams.put("interval", rInterval);
        }

        String baseUrl = buildBaseUrl(runContext);
        String endpoint = baseUrl + "/loki/api/v1/query_range";
        URI uri = buildUri(endpoint, queryParams);

        logger.debug("Querying Loki: {}", uri);

        HttpResponse<String> res = executeGetReq(runContext, uri);

        ObjectMapper objectMapper = new ObjectMapper();
        LokiQueryResponse lokiQueryResponse = objectMapper.readValue(res.getBody(), LokiQueryResponse.class);

        List<Map<String, Object>> logs = lokiQueryResponse.toLogEntries();

        logger.info("Retrieved {} log entries from Loki", logs.size());

        // Emit metrics
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
            title = "List of log entries",
            description = "Each entry contains timestamp, labels, and log line"
        )
        private final List<Map<String, Object>> logs;

        @Schema(
            title = "Result type",
            description = "Type of result returned by Loki (streams or matrix)"
        )
        private final String resultType;
    }
}
