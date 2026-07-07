# Setup monitoring { #configuration-server-monitoring }

REST API server provides the following endpoints with Prometheus compatible metrics:

- `GET /monitoring/metrics` - server metrics, like number of requests per path and response status, CPU and RAM usage, and so on.

These endpoints are enabled and configured using settings below:

::: data_rentgen.server.settings.monitoring.MonitoringSettings
