# How to use the Grafana plugin

Query logs from Grafana Loki from Kestra flows.

## Authentication

Set `url` to your Loki endpoint (e.g. `http://localhost:3100`). For authenticated deployments, set `authToken` to the Bearer token value. For multi-tenant Loki clusters, set `tenantId` (sent as the `X-Scope-OrgID` header). Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`loki.Query` runs an instant LogQL query set in `query`. Optionally set `time` to evaluate at a specific point (nanosecond epoch or RFC3339). Control the number of returned entries with `limit` (default 100) and sort order with `direction` (`BACKWARD` by default). The output includes `logs` and `resultType`.

`loki.QueryRange` runs a LogQL range query — set `start` and `end` (nanosecond epoch or RFC3339), or use `since` as a duration offset from `end` to compute `start`. Set `step` for matrix/metric query resolution and `interval` for stream sampling.

`loki.Trigger` polls Loki on a schedule (default 1 minute) and starts one execution per batch of log entries. Set `since` to control how far back to backfill on the first run (default `10m`). Uses stateful deduplication to avoid reprocessing entries across polls.
