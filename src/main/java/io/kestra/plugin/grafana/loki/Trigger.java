package io.kestra.plugin.grafana.loki;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.grafana.loki.models.LokiQueryResponse;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when a Loki query returns new results",
    description = "Polls Loki at regular intervals with a LogQL query and triggers a flow execution when new log entries matching the query are found. " +
        "The trigger maintains state to track processed logs and only fires on new entries. " +
        "Ideal for SecOps, SOAR, alerting, and monitoring use cases."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger on security alerts",
            full = true,
            code = """
                id: security_alert_handler
                namespace: security

                tasks:
                  - id: handle_alert
                    type: io.kestra.plugin.core.log.Log
                    message: "Security alert: {{ trigger.count }} new entries detected"

                  - id: process_logs
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.logs }}"

                triggers:
                  - id: watch_security_logs
                    type: io.kestra.plugin.grafana.loki.Trigger
                    url: http://loki.example.com:3100
                    authToken: "{{ secret('LOKI_TOKEN') }}"
                    tenantId: production
                    query: '{job="security", level="critical"} |= "unauthorized access"'
                    interval: PT1M
                    maxRecords: 100
                """
        ),
        @Example(
            title = "Trigger on error patterns with authentication",
            full = true,
            code = """
                id: error_monitor
                namespace: monitoring

                tasks:
                  - id: send_alert
                    type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
                    url: "{{ secret('SLACK_WEBHOOK') }}"
                    payload: |
                      {
                        "text": "🚨 {{ trigger.count }} errors detected",
                        "blocks": [
                          {
                            "type": "section",
                            "text": {
                              "type": "mrkdwn",
                              "text": "*Query:* {{ trigger.query }}"
                            }
                          }
                        ]
                      }

                triggers:
                  - id: monitor_errors
                    type: io.kestra.plugin.grafana.loki.Trigger
                    url: https://loki.example.com:3100
                    authToken: "{{ secret('LOKI_TOKEN') }}"
                    tenantId: team-platform
                    query: '{job="api", level="error"} |~ "timeout|connection refused"'
                    interval: PT5M
                    since: 10m
                """
        ),
        @Example(
            title = "Trigger on payment failures (SOAR use case)",
            code = """
                id: payment_failure_handler
                namespace: payments

                triggers:
                  - id: watch_payment_failures
                    type: io.kestra.plugin.grafana.loki.Trigger
                    url: http://loki:3100
                    tenantId: payments-team
                    query: '{application="payment-gateway"} |= "payment failed" | json | amount > 1000'
                    interval: PT30S
                    maxRecords: 50
                    since: 5m

                tasks:
                  - id: investigate
                    type: io.kestra.plugin.core.log.Log
                    message: "Investigating {{ trigger.count }} high-value payment failures"
                """
        )
    }
)
public class Trigger extends AbstractLokiTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {

    @Schema(
        title = "LogQL query to monitor",
        description = "The LogQL query to execute. When this query returns new results, the flow will be triggered."
    )
    @NotNull
    private Property<String> query;

    @Schema(
        title = "Polling interval",
        description = "How often to check for new logs. Defaults to every 1 minute."
    )
    @Builder.Default
    private Duration interval = Duration.ofMinutes(1);

    @Schema(
        title = "Maximum records per trigger",
        description = "Maximum number of log entries to return per trigger execution. Defaults to 100."
    )
    @Builder.Default
    private Property<Integer> maxRecords = Property.ofValue(100);

    @Schema(
        title = "Lookback window",
        description = "Time window to look back for logs on first run (e.g., '1h', '30m', '1d'). Defaults to 10 minutes."
    )
    @Builder.Default
    private Property<String> since = Property.ofValue("10m");

    @Schema(
        title = "State TTL",
        description = "Time to live for the trigger state. After this duration, the state will be cleared. Defaults to 1 day."
    )
    @Builder.Default
    private Property<Duration> stateTtl = Property.ofValue(Duration.ofDays(1));

    @Schema(
        title = "Custom state key",
        description = "Custom key for storing trigger state. If not provided, defaults to namespace.flow_id.trigger_id"
    )
    private Property<String> stateKey;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        String rQuery = runContext.render(query).as(String.class).orElseThrow();
        Integer rMaxRecords = runContext.render(maxRecords).as(Integer.class).orElse(100);
        String rSince = runContext.render(since).as(String.class).orElse("10m");

        var rStateKey = runContext.render(stateKey).as(String.class).orElse(defaultKey(context.getNamespace(), context.getFlowId(), context.getTriggerId()));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        Map<String, StatefulTriggerService.Entry> state = readState(runContext, rStateKey, rStateTtl);

        String queryStart = null;
        String queryEnd = String.valueOf(Instant.now().toEpochMilli() * 1_000_000); // Current time in nanoseconds

        // Build query parameters
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("query", rQuery);
        queryParams.put("limit", String.valueOf(rMaxRecords));
        queryParams.put("direction", "forward"); // Always forward to get oldest first
        queryParams.put("since", rSince);
        queryParams.put("end", queryEnd);

        // Execute query
        String baseUrl = buildBaseUrl(runContext);
        String endpoint = baseUrl + "/loki/api/v1/query_range";
        URI uri = buildUri(endpoint, queryParams);

        logger.debug("Polling Loki: {}", uri);

        HttpResponse<String> res = executeGetReq(runContext, uri);

        ObjectMapper objectMapper = new ObjectMapper();
        LokiQueryResponse lokiQueryResponse;
        Object body = res.getBody();
        if (body instanceof String) {
            lokiQueryResponse = objectMapper.readValue((String) body, LokiQueryResponse.class);
        } else {
            lokiQueryResponse = objectMapper.convertValue(body, LokiQueryResponse.class);
        }

        List<Map<String, Object>> logs = lokiQueryResponse.toLogEntries();

        if (logs.isEmpty()) {
            logger.debug("No logs found");
            return Optional.empty();
        }

        logger.debug("Found {} potential log entries", logs.size());

        List<Map<String, Object>> toFire = logs.stream()
            .filter(log -> {
                try {
                    String timestamp = (String) log.get("timestamp");
                    // Create a unique ID for the log entry
                    // We use timestamp + hash of content/labels to be unique
                    String content = log.containsKey("line") ? (String) log.get("line") : String.valueOf(log.get("value"));
                    Map<?, ?> labels = (Map<?, ?>) log.get("labels");
                    Entry candidate = getEntry(timestamp, content, labels);

                    // Check if we should fire for this entry
                    return computeAndUpdateState(state, candidate, On.CREATE).fire();
                } catch (Exception e) {
                    logger.warn("Failed to process log entry for state tracking", e);
                    return false;
                }
            })
            .toList();

        writeState(runContext, rStateKey, state, rStateTtl);

        if (toFire.isEmpty()) {
            logger.debug("No new logs found after state evaluation");
            return Optional.empty();
        }

        logger.info("Found {} new log entries", toFire.size());

        // Find the latest timestamp from the results
        String latestTimestamp = toFire.stream()
            .map(log -> (String) log.get("timestamp"))
            .filter(Objects::nonNull)
            .max(Comparator.naturalOrder())
            .orElse(queryEnd);

        Output output = Output.builder()
            .logs(toFire)
            .count(toFire.size())
            .query(rQuery)
            .resultType(lokiQueryResponse.getData().getResultType())
            .lastTimestamp(latestTimestamp)
            .build();

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }

    private static Entry getEntry(String timestamp, String content, Map<?, ?> labels) {
        String uniqueId = timestamp + "_" + (content + labels).hashCode();

        Instant recordTimestamp = Instant.ofEpochSecond(
            Long.parseLong(timestamp) / 1_000_000_000,
            Long.parseLong(timestamp) % 1_000_000_000
        );

        return Entry.candidate(
            uniqueId,
            timestamp,
            recordTimestamp
        );
    }

    @Override
    public Property<On> getOn() {
        return Property.ofValue(On.CREATE);
    }

    @Builder
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of new log entries that triggered the flow",
            description = "Each entry contains timestamp, labels, and log line or metric value"
        )
        private List<Map<String, Object>> logs;

        @Schema(
            title = "Number of new log entries",
            description = "Total count of logs that matched the query since last check"
        )
        private Integer count;

        @Schema(
            title = "Query executed",
            description = "The LogQL query that was executed"
        )
        private String query;

        @Schema(
            title = "Result type",
            description = "Type of result returned by Loki (streams, matrix, or vector)"
        )
        private String resultType;

        @Schema(
            title = "Latest timestamp",
            description = "Timestamp of the most recent log entry (in nanoseconds)"
        )
        private String lastTimestamp;
    }
}
