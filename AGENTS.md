# Kestra Grafana Plugin

## What

- Provides plugin components under `io.kestra.plugin.grafana.loki`.
- Includes classes such as `QueryRange`, `Trigger`, `LokiHttpService`, `Query`.

## Why

- This plugin integrates Kestra with Grafana.
- It provides tasks that query Grafana Loki and react to log and metric results with LogQL.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `grafana`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.grafana.loki.Query`
- `io.kestra.plugin.grafana.loki.QueryRange`
- `io.kestra.plugin.grafana.loki.Trigger`

### Project Structure

```
plugin-grafana/
├── src/main/java/io/kestra/plugin/grafana/loki/
├── src/test/java/io/kestra/plugin/grafana/loki/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
