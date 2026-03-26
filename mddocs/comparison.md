(comparison)=

# Comparison with other tools

## Why not [DataHub](https://datahubproject.io/)?

### DataHub cons

- As Data Catalog, DataHub relies on database ingestion mechanism.
  To extract and draw lineage between tables, it is required to *both* connect ingestor to all databases, and to enable integration with ETL (Spark, Airflow, etc).

  There is an option `spark.datahub.metadata.dataset.materialize=true`, but in this case DataHub creates datasets without schema,
  so ingestors are still required for column lineage.

- DataHub Spark agent doesn't properly work if *Platform Instances* are enabled in DataHub.
  Platform Instance is an additional hierarchy level for databases,
  and there is no way to map it to database address used by Spark, Airflow and other ETL tools.

- OpenLineage → DataHub integration collects each Spark command as dedicated *Pipeline Task*, producing a huge lineage graph.

  Data.Rentgen has configurable `granularity` option while rendering the lineage graph.

- No support for Job → Job hierarchy like Airflow Task → Spark application, or Airflow Task → Airflow Task dependencies.

- High CPU and memory consumption.

### DataHub pros

- DataHub has information about real dataset column names, types, description.
  Data.Rentgen has only information provided by ETL engine, e.g. selected columns, ETL engine-specific column types.
- DataHub has table → view lineage, Data.Rentgen doesn't.

## Why not [OpenMetadata](https://open-metadata.org/)?

### OpenMetadata cons

- Database ingestors are required to build a lineage graph, just like DataHub.
- OpenLineage → OpenMetadata integration produces no lineage, for some unknown reason.
- No support for Job → Job hierarchy like Airflow Task → Spark application, or Airflow Task → Airflow Task dependencies.
- High CPU and memory consumption.

### OpenMetadata pros

- OpenMetadata has information about real dataset column names, types, description.

  Data.Rentgen has only information available in ETL engine, e.g. selected columns, ETL engine-specific column types.

- OpenMetadata has table → view lineage, Data.Rentgen doesn't.

## Why not [Marquez](https://marquezproject.ai/)?

### Marquez cons

- OpenLineage → Marquez integration collects each Spark command as dedicated Jobs, producing too detailed lineage graph.

  Data.Rentgen has configurable `granularity` option while rendering the lineage graph.

- Severe performance issues while consuming lineage events.
- No support for dataset symlinks, e.g. HDFS location → Hive table.
- No support for Job → Job hierarchy like Airflow Task → Spark application, or Airflow Task → Airflow Task dependencies.
- No releases since 2024.

### Marquez pros

- Marquez store and show lineage for any OpenLineage integration.
  Data.Rentgen may require some adjustments for that.

- Marquez store and show any facet produced by OpenLineage integration, including custom ones.
  Data.Rentgen stores only selected facets.

## Why not [Apache Atlas](https://atlas.apache.org)?

- No Apache Spark 3.x integration in open source.
- Only Apache Airflow 1.x integration, but no 2.x and 3.x support.
- High CPU and memory consumption in production environment, as it uses HBase as storage layer.

## Why not [Open Data Discovery](https://opendatadiscovery.org/)?

- No Apache Spark integration.
- Only Apache Airflow 1.x integration, but no 2.x and 3.x support.

## Why not [Amudsen](https://www.amundsen.io)?

- No Apache Spark integration.
- No releases since 2024.

## Why not [Spline](https://absaoss.github.io/spline/)?

- No Apache Airflow integration.
- ArangoDB changed license from Apache-2.0 to BSL [since 2024.02.19](https://arangodb.com/2024/02/update-evolving-arangodbs-licensing-model-for-a-sustainable-future/).

## Why not [Egeria](https://egeria-project.org/)?

Insanely complicated.
