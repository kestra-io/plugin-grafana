package io.kestra.plugin.grafana.loki;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static java.net.URLEncoder.encode;

public class LokiHttpService {

    public static HttpClient createClient(RunContext runContext, Property<Integer> connectTimeout) throws IllegalVariableEvaluationException {
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

    public static HttpRequest.HttpRequestBuilder buildRequest(
        RunContext runContext,
        URI uri,
        Property<String> authToken,
        Property<String> tenantId
    ) throws IllegalVariableEvaluationException {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(uri);

        if (authToken != null) {
            runContext.render(authToken).as(String.class)
                .ifPresent(token -> requestBuilder.addHeader("Authorization", "Bearer " + token));
        }

        if (tenantId != null) {
            runContext.render(tenantId).as(String.class)
                .ifPresent(tenant -> requestBuilder.addHeader("X-Scope-OrgID", tenant));
        }

        requestBuilder.addHeader("Content-Type", "application/json");
        requestBuilder.addHeader("Accept", "application/json");

        return requestBuilder;
    }

    public static HttpResponse<String> executeGetRequest(
        RunContext runContext,
        URI uri,
        Property<String> authToken,
        Property<String> tenantId,
        Property<Integer> connectTimeout
    ) throws Exception {
        HttpClient client = createClient(runContext, connectTimeout);
        HttpRequest request = buildRequest(runContext, uri, authToken, tenantId)
            .method("GET")
            .build();

        HttpResponse<String> res = client.request(request);

        if (res.getStatus().getCode() < 200 || res.getStatus().getCode() >= 300) {
            throw new RuntimeException(
                String.format("Loki API request failed with status %d: %s",
                    res.getStatus().getCode(),
                    res.getBody())
            );
        }
        return res;
    }

    public static HttpResponse<String> executePostRequest(
        RunContext runContext,
        URI uri,
        Property<String> authToken,
        Property<String> tenantId,
        Property<Integer> connectTimeout
    ) throws Exception {
        HttpClient client = createClient(runContext, connectTimeout);
        HttpRequest request = buildRequest(runContext, uri, authToken, tenantId)
            .method("POST")
            .build();

        HttpResponse<String> res = client.request(request);

        if (res.getStatus().getCode() < 200 || res.getStatus().getCode() >= 300) {
            throw new RuntimeException(
                String.format("Loki API request failed with status %d: %s",
                    res.getStatus().getCode(),
                    res.getBody())
            );
        }
        return res;
    }

    public static String buildBaseUrl(RunContext runContext, Property<String> url) throws IllegalVariableEvaluationException {
        String renderedUrl = runContext.render(url).as(String.class).orElseThrow();
        return renderedUrl.replaceAll("/$", "");
    }

    public static URI buildUri(String endpoint, Map<String, String> queryParams) {
        StringBuilder uriBuilder = new StringBuilder(endpoint);

        if (queryParams != null && !queryParams.isEmpty()) {
            uriBuilder.append("?");
            boolean first = true;
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                if (!first) {
                    uriBuilder.append("&");
                }
                uriBuilder.append(entry.getKey())
                    .append("=")
                    .append(encode(entry.getValue(), StandardCharsets.UTF_8));
                first = false;
            }
        }

        return URI.create(uriBuilder.toString());
    }
}
