{{ datarentgen_logo_wide }}

[![Repo Status](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip) [![Docker image](https://img.shields.io/docker/v/mtsrus/data-rentgen?sort=semver&label=docker)](https://hub.docker.com/r/mtsrus/data-rentgen) [![PyPI](https://img.shields.io/pypi/v/data-rentgen)](https://pypi.org/project/data-rentgen/) [![PyPI License](https://img.shields.io/pypi/l/data-rentgen.svg)](https://github.com/MobileTeleSystems/data-rentgen/blob/develop/LICENSE.txt) [![PyPI Python Version](https://img.shields.io/pypi/pyversions/data-rentgen.svg)](https://badge.fury.io/py/data-rentgen) [![Documentation](https://readthedocs.org/projects/data-rentgen/badge/?version=stable)](https://data-rentgen.readthedocs.io/)
[![Build Status](https://github.com/MobileTeleSystems/data-rentgen/workflows/Tests/badge.svg)](https://github.com/MobileTeleSystems/data-rentgen/actions) [![Coverage](https://codecov.io/github/MobileTeleSystems/data-rentgen/graph/badge.svg?token=s0JztGZbq3)](https://codecov.io/github/MobileTeleSystems/data-rentgen) [![pre-commit.ci](https://results.pre-commit.ci/badge/github/MobileTeleSystems/data-rentgen/develop.svg)](https://results.pre-commit.ci/latest/github/MobileTeleSystems/data-rentgen/develop)

# What is Data.Rentgen?

Data.Rentgen is a Data Motion Lineage service, compatible with [OpenLineage](https://openlineage.io/) specification.

Currently we support consuming lineage from:

* Apache Spark
* Apache Airflow
* Apache Hive
* Apache Flink
* dbt

**Note**: service is under active development, so it doesn’t have stable API for now.

# Goals

* Collect lineage events produced by OpenLineage clients & integrations.
* Store operation-grained events for better detalization (instead of job grained [Marquez](https://marquezproject.ai/)).
* Provide API for fetching both job/run ↔ dataset lineage and dataset ↔ dataset lineage.

# Features

* Support consuming large amounts of lineage events, use Apache Kafka as event buffer.
* Store data in tables partitioned by event timestamp, to speed up lineage graph resolution.
* Lineage graph is build with user-specified time boundaries (unlike Marquez where lineage is build only for last job run).
* Lineage graph can be build with different granularity. e.g. merge all individual Spark commands into Spark applicationId or Spark applicationName.
* Column-level lineage support.
* Authentication support.

# Non-goals

* This is **not** a Data Catalog. DataRentgen doesn’t track dataset schema change, owner and so on. Use [Datahub](https://datahubproject.io/) or [OpenMetadata](https://open-metadata.org/) instead.
* Static Data Lineage like view → table is not supported.

# Limitations

* OpenLineage have integrations with Trino, Debezium and some other lineage sources. DataRentgen support may be added later.
* Unlike Marquez, DataRentgen parses only limited set of facets send by OpenLineage, and doesn’t store custom facets. This can be changed in future.

# Screenshots

## Lineage graph

Dataset-level lineage graph

![Dataset-level lineage graph](entities/dataset_lineage.png)

Dataset column-level lineage graph

![Dataset column-level lineage graph](entities/dataset_column_lineage.png)

Job-level lineage graph

![Job-level lineage graph](entities/job_lineage.png)

Run-level lineage graph

![Job-level lineage graph](entities/run_lineage.png)

## Datasets

![Datasets list](entities/dataset_list.png)

## Runs

![Runs list](entities/run_list.png)

## Spark application

![Spark application details](integrations/spark/job_details.png)

## Spark run

![Spark run details](integrations/spark/run_details.png)

## Spark command

![Spark command details](integrations/spark/operation_details.png)

## Hive query

![Hive query details](integrations/hive/operation_details.png)

## Airflow DagRun

![Airflow DagRun details](integrations/airflow/dag_run_details.png)

## Airflow TaskInstance

![Airflow TaskInstance details](integrations/airflow/task_run_details.png)
