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
        @JsonProperty("stream")
        private Map<String, String> stream;

        @JsonProperty("metric")
        private Map<String, String> metric;

        // For stream and matrix responses (array of [timestamp, value] arrays)
        private List<List<String>> values;

        // For vector responses (single [timestamp, value] array)
        private List<String> value;
    }

    public List<Map<String, Object>> toLogEntries() {
        List<Map<String, Object>> entries = new ArrayList<>();

        if (data != null && data.result != null) {
            for (Result result : data.result) {
                // Determine which labels to use (stream or metric)
                Map<String, String> labels = result.stream != null ? result.stream : result.metric;

                // Handle vector type (single value array)
                if (result.value != null) {
                    Map<String, Object> entry = new HashMap<>();
                    entry.put("timestamp", result.value.get(0));
                    entry.put("value", result.value.get(1));
                    entry.put("labels", labels);
                    entries.add(entry);
                }
                // Handle stream and matrix types (array of value arrays)
                else if (result.values != null) {
                    for (List<String> value : result.values) {
                        Map<String, Object> entry = new HashMap<>();
                        entry.put("timestamp", value.get(0));

                        // For stream responses, the second value is the log line
                        // For matrix responses, the second value is the metric value
                        if (result.stream != null) {
                            entry.put("line", value.get(1));
                        } else {
                            entry.put("value", value.get(1));
                        }

                        entry.put("labels", labels);
                        entries.add(entry);
                    }
                }
            }
        }

        return entries;
    }
}