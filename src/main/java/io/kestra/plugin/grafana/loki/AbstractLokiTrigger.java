package io.kestra.plugin.grafana.loki;

import java.net.URI;
import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
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
public abstract class AbstractLokiTrigger extends AbstractTrigger {

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
