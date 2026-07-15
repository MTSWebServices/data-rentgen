# Setup monitoring { #configuration-consumer-monitoring }

Consumer provides the following endpoints with Prometheus compatible metrics:

- `GET /monitoring/metrics` - consumer metrics, like number of received/processed messages per handler, processing duration and exceptions raised while processing (e.g. database conflicts, deadlocks or unavailability), and so on. See [FastStream Prometheus](https://faststream.ag2.ai/latest/getting-started/observability/prometheus/) documentation for the full list of metrics.

These endpoints are enabled and configured using settings below:

::: data_rentgen.consumer.settings.monitoring.MonitoringSettings
