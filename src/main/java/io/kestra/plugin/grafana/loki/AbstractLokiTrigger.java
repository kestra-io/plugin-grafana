package io.kestra.plugin.grafana.loki;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.Map;

@SuperBuilder
@NoArgsConstructor
@ToString
@Getter
@EqualsAndHashCode
public abstract class AbstractLokiTrigger extends AbstractTrigger {

    @Schema(
        title = "Loki base URL",
        description = "The base URL of your Loki instance (e.g., http://localhost:3100 or https://logs.example.com)"
    )
    @NotNull
    protected Property<String> url;

    @Schema(
        title = "Authentication token",
        description = "Bearer token for authentication if Loki is secured"
    )
    protected Property<String> authToken;

    @Schema(
        title = "Grafana Loki Tenant ID",
        description = "X-Scope-OrgID header value for multi-tenant Loki setups"
    )
    protected Property<String> tenantId;

    @Schema(
        title = "Connection timeout",
        description = "HTTP connection timeout in seconds"
    )
    @Builder.Default
    protected Property<Integer> connectTimeout = Property.ofValue(30);

    @Schema(
        title = "Read timeout",
        description = "HTTP read timeout in seconds"
    )
    @Builder.Default
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
