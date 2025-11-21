package io.kestra.plugin.grafana.loki.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class LokiQueryResponse {
    private String status;
    private Data data;

    @lombok.Data
    public static class Data {
        private String resultType;
        private List<Result> result;
    }

    @lombok.Data
    public static class Result {
        private Map<String, String> stream;
        private List<List<String>> values;
    }

    public List<Map<String, Object>> toLogEntries() {
        List<Map<String, Object>> entries = new ArrayList<>();

        if (data != null && data.result != null) {
            for (Result result : data.result) {
                if (result.values != null) {
                    for (List<String> value : result.values) {
                        Map<String, Object> entry = new HashMap<>();
                        entry.put("timestamp", value.get(0));
                        entry.put("line", value.get(1));
                        entry.put("labels", result.stream);
                        entries.add(entry);
                    }
                }
            }
        }

        return entries;
    }
}