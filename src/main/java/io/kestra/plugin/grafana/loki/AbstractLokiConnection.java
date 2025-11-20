package io.kestra.plugin.grafana.loki;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;

@SuperBuilder
@NoArgsConstructor
@ToString
@Getter
@EqualsAndHashCode
public abstract class AbstractLokiConnection extends Task {

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
        title = "Tenant ID",
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

    protected HttpClient createClient(RunContext runContext) throws IllegalVariableEvaluationException {
        Integer rConnectionTimeout = runContext.render(connectTimeout).as(Integer.class).orElse(30);

        HttpConfiguration configuration = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder()
                .connectTimeout(Property.ofValue(Duration.ofSeconds(rConnectionTimeout)))
                .build())
            .build();

        return HttpClient.builder()
            .runContext(runContext)
            .configuration(configuration)
            .build();
    }

    protected HttpRequest.HttpRequestBuilder requestBuilder(RunContext runContext, URI uri) throws IllegalVariableEvaluationException {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(uri);

        if (authToken != null) {
            runContext.render(authToken).as(String.class).ifPresent(token -> requestBuilder.addHeader("Authorization", "Bearer " + token));
        }

        if (tenantId != null) {
            runContext.render(tenantId).as(String.class).ifPresent(tenant -> requestBuilder.addHeader("X-Scope-OrgID", tenant));
        }

        requestBuilder.addHeader("Content-Type", "application/json");
        requestBuilder.addHeader("Accept", "application/json");

        return requestBuilder;
    }

    protected HttpResponse<String> executeGetReq(RunContext runContext, URI uri) throws Exception {
        HttpClient client = createClient(runContext);
        HttpRequest request = requestBuilder(runContext, uri)
            .method("GET")
            .build();

        HttpResponse<String> res = client.request(request);

        if (res.getStatus().getCode() < 200 || res.getStatus().getCode() >=300) {
            throw new RuntimeException(
                String.format("Loki API request failed with status %d: %s",
                    res.getStatus().getCode(),
                    res.getBody())
            );
        }
        return res;
    }

    protected HttpResponse<String> executePostReq(RunContext runContext, URI uri) throws Exception {
        HttpClient client = createClient(runContext);
        HttpRequest request = requestBuilder(runContext, uri)
            .method("POST")
            .build();

        HttpResponse<String> res = client.request(request);
        if (res.getStatus().getCode() < 200 || res.getStatus().getCode() >=300) {
            throw new RuntimeException(
                String.format("Loki API request failed with status %d: %s",
                    res.getStatus().getCode(),
                    res.getBody())
            );
        }
        return res;
    }

    protected String buildBaseUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        String renderedUrl = runContext.render(this.url).as(String.class).orElseThrow();
        return renderedUrl.replaceAll("/$", "");
    }

    protected URI buildUri(String endpoint, java.util.Map<String, String> queryParams) {
        StringBuilder uriBuilder = new StringBuilder(endpoint);

        if (queryParams != null && !queryParams.isEmpty()) {
            uriBuilder.append("?");
            boolean first = true;
            for (java.util.Map.Entry<String, String> entry : queryParams.entrySet()) {
                if (!first) {
                    uriBuilder.append("&");
                }
                uriBuilder.append(entry.getKey())
                    .append("=")
                    .append(java.net.URLEncoder.encode(entry.getValue(), java.nio.charset.StandardCharsets.UTF_8));
                first = false;
            }
        }

        return URI.create(uriBuilder.toString());
    }

}
