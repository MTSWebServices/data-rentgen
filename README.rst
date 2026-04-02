.. _readme:

|Logo|

.. |Logo| image:: docs/_static/logo_wide_white_text.svg
    :alt: Data.Rentgen logo
    :target: https://github.com/MTSWebServices/data-rentgen

|Repo Status| |Docker image| |PyPI| |PyPI License| |PyPI Python Version| |Documentation|
|Build Status| |Coverage| |pre-commit.ci|

.. |Repo Status| image:: https://www.repostatus.org/badges/latest/wip.svg
    :target: https://www.repostatus.org/#wip
.. |Docker image| image:: https://img.shields.io/docker/v/mtsrus/data-rentgen?sort=semver&label=docker
    :target: https://hub.docker.com/r/mtsrus/data-rentgen
.. |PyPI| image:: https://img.shields.io/pypi/v/data-rentgen
    :target: https://pypi.org/project/data-rentgen/
.. |PyPI License| image:: https://img.shields.io/pypi/l/data-rentgen.svg
    :target: https://github.com/MTSWebServices/data-rentgen/blob/develop/LICENSE.txt
.. |PyPI Python Version| image:: https://img.shields.io/pypi/pyversions/data-rentgen.svg
    :target: https://badge.fury.io/py/data-rentgen
.. |Documentation| image:: https://readthedocs.org/projects/data-rentgen/badge/?version=stable
    :target: https://data-rentgen.readthedocs.io/
.. |Build Status| image:: https://github.com/MTSWebServices/data-rentgen/workflows/Tests/badge.svg
    :target: https://github.com/MTSWebServices/data-rentgen/actions
.. |Coverage| image:: https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/
    MTSOnGithub/03e73a82ecc4709934540ce8201cc3b4/raw/data-rentgen_badge.json
    :target: https://github.com/MTSWebServices/data-rentgen/actions
.. |pre-commit.ci| image:: https://results.pre-commit.ci/badge/github/MTSWebServices/data-rentgen/develop.svg
    :target: https://results.pre-commit.ci/latest/github/MTSWebServices/data-rentgen/develop

What is Data.Rentgen?
---------------------

Data.Rentgen is a Data Motion Lineage service, compatible with `OpenLineage <https://openlineage.io/>`_ specification.

Currently we support consuming lineage from:

* Apache Spark
* Apache Airflow
* Apache Hive
* Apache Flink
* dbt
* SyncMaster (proprietary integration, part of MWS Data Bridge)
* StarRocks (proprietary integration, part of MWS Data Engine)

**Note**: service is under active development, so API can be unstable.

Goals
-----

* Collect lineage events produced by OpenLineage clients & integrations.
* Store operation-grained events for better detalization.
* Provide API for fetching both job/run ↔ dataset lineage and dataset ↔ dataset lineage.

Features
--------

* Support consuming large amounts of lineage events, use Apache Kafka as event buffer.
* Store data in tables partitioned by event timestamp, to speed up lineage graph resolution.
* Lineage graph is build with user-specified time boundaries.
* Lineage graph can be build with different granularity. e.g. merge all individual Spark commands into Spark applicationId or Spark applicationName.
* Column-level lineage support.
* Authentication support.

Non-goals
---------

* This is **not** a Data Catalog. DataRentgen doesn't track dataset schema change, owner and so on. Use `Datahub <https://datahubproject.io/>`_ or `OpenMetadata <https://open-metadata.org/>`_ instead.
* Static Data Lineage like view → table is not supported.

Limitations
-----------

* OpenLineage have integrations with Trino, Debezium and some other lineage sources. DataRentgen support may be added later.
* DataRentgen parses only limited set of OpenLineage facets, and doesn't store custom facets. This can be changed in future.

.. documentation

Documentation
-------------

See https://data-rentgen.readthedocs.io/

Screenshots
-----------

Lineage graph
~~~~~~~~~~~~~

Dataset-level lineage graph

.. image:: docs/entities/dataset_lineage.png
    :alt: Dataset-level lineage graph

Dataset column-level lineage graph

.. image:: docs/entities/dataset_column_lineage.png
    :alt: Dataset column-level lineage graph

Job-level lineage graph

.. image:: docs/entities/job_lineage.png
    :alt: Job-level lineage graph

Run-level lineage graph

.. image:: docs/entities/run_lineage.png
    :alt: Job-level lineage graph

Hierarchy graph
~~~~~~~~~~~~~~~

.. image:: docs/integrations/airflow/job_hierarchy.png
    :alt: Job hierarchy

Datasets
~~~~~~~~

.. image:: docs/entities/dataset_list.png
    :alt: Datasets list

Runs
~~~~

.. image:: docs/entities/run_list.png
    :alt: Runs list

Spark application
~~~~~~~~~~~~~~~~~

.. image:: docs/integrations/spark/job_details.png
    :alt: Spark application details

Spark run
~~~~~~~~~

.. image:: docs/integrations/spark/run_details.png
    :alt: Spark run details

Spark command
~~~~~~~~~~~~~~~

.. image:: docs/integrations/spark/operation_details.png
    :alt: Spark command details

Hive query
~~~~~~~~~~

.. image:: docs/integrations/hive/operation_details.png
    :alt: Hive query details

Airflow DagRun
~~~~~~~~~~~~~~

.. image:: docs/integrations/airflow/dag_run_details.png
    :alt: Airflow DagRun details

Airflow TaskInstance
~~~~~~~~~~~~~~~~~~~~

.. image:: docs/integrations/airflow/task_run_details.png
    :alt: Airflow TaskInstance details
