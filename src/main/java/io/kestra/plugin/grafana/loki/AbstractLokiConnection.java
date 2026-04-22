package io.kestra.plugin.grafana.loki;

import java.net.URI;
import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@NoArgsConstructor
@ToString
@Getter
@EqualsAndHashCode
public abstract class AbstractLokiConnection extends Task {

    @Schema(
        title = "Loki base URL",
        description = "HTTPS endpoint of the Loki API, including scheme (e.g., `http://localhost:3100` or `https://logs.example.com`)"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> url;

    @Schema(
        title = "Bearer token",
        description = "Authorization header value for secured Loki deployments; render from secrets when possible"
    )
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> authToken;

    @Schema(
        title = "Tenant ID",
        description = "X-Scope-OrgID header used by multi-tenant Loki clusters"
    )
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(
        title = "HTTP connect timeout",
        description = "Connection timeout in seconds; defaults to 30"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> connectTimeout = Property.ofValue(30);

    @Schema(
        title = "HTTP read timeout",
        description = "Read timeout in seconds; defaults to 60"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> readTimeout = Property.ofValue(60);

    @Schema(
        title = "LogQL query",
        description = "Rendered LogQL expression sent to Loki (e.g., `'{job=\"api\"} |= \"error\"'`)"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> query;

    @Schema(
        title = "Result limit",
        description = "Maximum number of entries to return; defaults to 100"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> limit = Property.ofValue(100);

    @Schema(
        title = "Sort direction",
        description = "Use FORWARD for ascending timestamps or BACKWARD for descending; defaults to BACKWARD"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Direction> direction = Property.ofValue(Direction.BACKWARD);

    public enum Direction {
        FORWARD,
        BACKWARD
    }

    protected HttpResponse<String> executeGetReq(RunContext runContext, URI uri) throws Exception {
        return LokiHttpService.executeGetRequest(runContext, uri, authToken, tenantId, connectTimeout);
    }

    protected String buildBaseUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        return LokiHttpService.buildBaseUrl(runContext, this.url);
    }

    protected URI buildUri(String endpoint, Map<String, String> queryParams) {
        return LokiHttpService.buildUri(endpoint, queryParams);
    }

}
