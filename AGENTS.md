# Kestra Grafana Plugin

## What

- Provides plugin components under `io.kestra.plugin.grafana.loki`.
- Includes classes such as `QueryRange`, `Trigger`, `LokiHttpService`, `Query`.

## Why

- What user problem does this solve? Teams need to query Grafana Loki and react to log and metric results with LogQL from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Grafana steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Grafana.

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
