.. _entities:

Entities
========

.. plantuml::

    @startuml
        title Entities diagram
        skinparam componentStyle rectangle
        left to right direction

        component Location1 {
            collections Addresses1
        }
        component Location2 {
            collections Addresses2
        }
        component Location3 {
            collections Addresses3
        }
        database Dataset1
        database Dataset2
        component Job
        collections Run
        collections Operation
        component User

        [Dataset1] --> [Location1]: located in
        [Dataset2] --> [Location2]: located in
        [Dataset1] --> [Dataset2]: SYMLINK
        [Dataset2] --> [Dataset1]: SYMLINK
        [Job] --> [Location3]: located in
        [Run] --> [Job]: PARENT
        [Operation] --> [Run]: PARENT
        [Run] --> [User]: started by
        [Run] --> [Run]: PARENT
        [Dataset1] ..> [Operation]: INPUT
        [Operation] ..> [Dataset1]: OUTPUT

    @enduml

Nodes
-----

Nodes are independent entities which describe information about some real entity, like table, ETL job, ETL job run and so on.

Location
~~~~~~~~

Represents information "where is dataset located", "where is job started from".
This is the analog of `OpenLineage namespace <https://openlineage.io/docs/spec/naming/>`_ concept.

Examples:

- ``hive://some-cluster``
- ``hdfs://some-cluster``
- ``oracle://some.host.name:1521``
- ``postgres://some.host.name:5432``
- ``yarn://some-cluster``
- ``local://some.host.name``
- ``http://airflow-web-ui.domain.com:8080``

It contains following fields:

- ``id: int`` - internal unique identifier.
- ``type: str`` - location type, e.g. ``hive``, ``hdfs``, ``oracle`` and so on.
- ``name: str`` - location name, e.g. ``some-cluster``, ``some.host.name``
- ``external_id: str | None`` - external identified of this location in some third-party system (e.g. PlatformInstance in `Datahub <https://datahubproject.io/>`_).
- ``addresses`` - list of alternative location addresses (see below):

  - ``url: str`` - alternative address, in URL form.

.. image:: location_list.png

Location addresses
^^^^^^^^^^^^^^^^^^

In real world, the same physical host or cluster may have multiple addresses, for example:

- PostgreSQL instance may be accessed by its host name ``postgres://some.host.name:5432`` or by IP ``postgres://192.128.20.14:5432``
- With or without port number - ``postgres://some.host.name:5432`` vs. ``postgres://some.host.name``

Also clusters like Hadoop, Kafka and so on, may be accessed by multiple host names:

- ``hdfs://some-cluster`` → ``[hdfs://some-cluster.name.node1:8082, hdfs://some-cluster.name.node2]``.
- ``kafka://bootstrap.server1,bootstrap.server2,bootstrap.server3`` → ``[kafka://bootstrap.server1,kafka://bootstrap.server2,kafka://bootstrap.server3]``.

Each Spark application run may connect to any of these addresses, and access the same data.

Having a list of alternative addresses of specific location allows to resolve this ambiguity, and always match the same physical table on the same cluster
to the same Data.Rentgen dataset. This prevents creating duplicates of dataset or job.

Dataset
~~~~~~~

Represents information about some table/topic/collection/folder, stored in specific location.

Examples:

- ``hive://some-cluster`` + ``myschema.mytable`` - table inside a Hive cluster.
- ``postgres://some.host.name:5432`` + ``mydb.myschema.mytable`` - table inside a Postgres instance.
- ``hdfs://some-cluster`` + ``/app/warehouse/hive/managed/myschema.db/mytable`` - folder inside a HDFS cluster.

Note that all information Data.Rentgen has was actually reported by ETL jobs, and not by database. There are no database integrations.

For example, Spark command read something from PostgreSQL object ``public.dataset_name``. This can be a table, a view, a foreign table - *we don't know*.

That's why the information about datasets is very limited:

- ``id: int`` - internal unique identifier.
- ``location: Location`` - Location where data is actually stored in, like RDMBS instance or cluster.
- ``name: str`` - qualified name of Dataset, like ``mydb.myschema.mytable`` or ``/app/warehouse/hive/managed/myschema.df/mytable``
- ``schema: Schema | None`` - schema of dataset.
- ``tags: list[Tag]`` - tags of dataset.

.. image:: dataset_list.png

Dataset schema
^^^^^^^^^^^^^^

Schema only exists as a part of some interaction, like Spark application written some dataframe to ORC file,
or Flink fetched some data from PostgreSQL table.

Also, there can be multiple schemas of dataset:

* If dataset is an input, it may contain only *selected* columns. We call this schema projection.
* If dataset is an output, the schema field usually represents actual table columns. Except ``DEFAULT`` or ``COMPUTED`` columns.
* If dataset is both input and output, we prefer using the output schema, because it has more information (like column types).

It contains following fields:

- ``id: int`` - internal unique identifier.
- ``fields: list[SchemaField]``:

  - ``name: str`` - column name
  - ``type: str | None`` - column type, if any.
    Note that this is types in ETL engine (Spark, Flink, etc), and not types of source (Postgres, Clickhouse).
  - ``description: str | None`` - column description/comment, if any.
  - ``fields: list[SchemaField]`` - if column contain nested fields (e.g. ``struct``, ``array``, ``map``).

- ``relevance_type: Enum`` - describes if this schema information is relevant:

  - ``EXACT_MATCH`` - returned if all interactions with this dataset used only one schema.
  - ``LATEST_KNOWN`` - if there are multiple interactions with this dataset, but with different schemas. In this case a schema of the most recent interaction is returned.

.. image:: dataset_schema.png

Job
~~~

Represents information about ETL job in specific location.
This is an abstraction to group by different runs of the same Spark application, Airflow DAG, Airflow Task, etc.

Examples:

- ``yarn://some-cluster`` + ``my-spark-session`` - Spark applicationName, running inside a YARN cluster (``master=yarn``).
- ``local://some.host.name`` + ``my-spark-session`` - Spark applicationName, running on a host (``master=local``).
- ``http://airflow-web-ui.domain.com:8080`` + ``my_dag`` - Airflow DAG, created in Airflow instance.
- ``http://airflow-web-ui.domain.com:8080`` + ``my_dag.mytask`` - Airflow Task within Airflow DAG, created in Airflow instance.
- ``http://flink.domain.com:18081`` + ``some_flink_application`` - Flink job running in Flink instance.
- ``local://some.host.name`` + ``my_project`` - dbt project running on a host.

It contains following fields:

- ``id: int`` - internal unique identifier.
- ``location: Location`` - Location where Job is run, e.g. cluster or host name.
- ``name: str`` - name of Job, like ``my-session-name``, ``mydag``, ``mydag.mytask``
- ``type: str`` - type of Job, like:

  - ``SPARK_APPLICATION``
  - ``AIRLOW_DAG``
  - ``AIRFLOW_TASK``
  - ``FLINK_JOB``
  - ``DBT_JOB``
  - ``UNKNOWN``

- ``parent_job_id: int`` - parent Job which started this specific Job, e.g. Spark applicationId was started by Airflow Task Instance, or Airflow Task Instance is a started by Airflow DagRun.
- ``tags: list[Tag]`` - tags of job.

.. image:: job_list.png

Tags
~~~~

Datasets and jobs can have multiple tags which are arbitrary ``key: value`` pairs.

- ``id: int`` - tag identifier
- ``name: str`` - tag name, usually in format ``source.name``, e.g. ``airflow.version``,  ``company.team``
- ``values: list[TagValue]`` - tag values bound to dataset/job:

  - ``id: int`` - tag value identifier
  -  ``value: str`` - tag value, e.g. ``1.3.4``, ``production``, ``Some team``

.. image:: tags.png

User
~~~~

Represents information about some user.

It contains following fields:

- ``id: bigint`` - internal unique identifier.
- ``name: str`` - username.

Run
~~~

Represents information about Job run:

- for Spark applicationName it is a Spark applicationId
- for Airflow DAG it is a DagRun
- for Airflow Task it is a TaskInstance
- for Apache Flink it is jobId
- for dbt it is ``dbt run`` instance

It contains following fields:

- ``id: uuidv7`` - unique identifier, generated on client.
- ``created_at: timestamp`` - extracted UUIDv7 timestamp, used for filtering purpose.
- ``job_id: int`` - bound to specific Job.
- ``parent_run_id: uuidv7`` - parent Run which started this specific Run, e.g. Spark applicationId was started by Airflow Task Instance, or Airflow Task Instance is a started by Airflow DagRun.
- ``started_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=START`` was received.
- ``started_by user: User | None`` - Spark session started as specific OS user/Kerberos principal.
- ``start_reason: Enum | None`` - "why this Run was started?":
- ``expected_start_at: timestamp | None`` - scheduled/expected start time of the run, which may differ from ``started_at`` (actual start time).
- ``expected_end_at: timestamp | None`` - scheduled/expected end time of the run, which may differ from ``ended_at`` (actual end time).

  - ``MANUAL``
  - ``AUTOMATIC`` - e.g. by schedule or triggered by another run.

- ``status: Enum`` - run status. Currently these statuses are supported:

  - ``UNKNOWN``
  - ``STARTED``
  - ``SUCCEEDED``
  - ``FAILED``
  - ``KILLED``

- ``ended_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=COMPLETE|FAIL|ABORT`` was received.
- ``ended_reason: str | None`` - reason of receiving this status, if it is ``FAILED`` or ``KILLED``.
- ``external_id : str | None`` - external identifier of this Run, e.g. Spark ``applicationId`` or Airflow ``dag_run_id``.
- ``attempt: str | None`` - external attempt number of this Run, e.g. Spark ``attemptId`` in YARN, or Airflow Task ``try_number``.
- ``running_log_url: str | None`` - external URL there specific Run information could be found (e.g. Spark UI).
- ``persistent_log_url: str | None`` - external URL there specific Run logs could be found (e.g. Spark History server, Airflow Web UI).

.. image:: run_list.png
.. image:: ../integrations/spark/run_details.png
.. image:: ../integrations/airflow/dag_run_details.png
.. image:: ../integrations/airflow/task_run_details.png

Operation
~~~~~~~~~

Represents specific Spark job or Spark execution information. For now, Airflow DAG and Airflow task does not have any operations.

It contains following fields:

- ``id: uuidv7`` - unique identifier, generated on client.
- ``created_at: timestamp`` - extracted UUIDv7 timestamp, used for filtering purpose.
- ``run_id: uuidv7`` - bound to specific Run.
- ``started_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=START`` was received.
- ``status: Enum`` - run status. Currently these statuses are supported:

  - ``UNKNOWN``
  - ``STARTED``
  - ``SUCCEEDED``
  - ``FAILED``
  - ``KILLED``

- ``ended_at: timestamp | None`` - timestamp when OpenLineage event with ``eventType=COMPLETE|FAIL|ABORT`` was received.
- ``name: str`` - name of operation, e.g. Spark command , dbt command name.
- ``position: int | None`` - positional number of operation, e.g. number of Spark execution in Spark UI or ``map_index`` of Airflow Task.
- ``group: str | None`` - field to group operations by, e.g. Spark job ``jobGroup`` or DBT command type (``MODEL``, ``SQL``, ``TEST``, ``SNAPSHOT``).
- ``description: str | None`` - operation description, e.g. Spark job ``jobDescription`` field, Airflow Operator name.
- ``sql_query: str | None`` - SQL query executed by this operation, if any.

.. image:: ../integrations/dbt/operation_details.png

Relations
---------

These entities describe relationship between different nodes.

Dataset Symlink
~~~~~~~~~~~~~~~

Represents dataset relations like ``Hive metastore table → HDFS/S3 location of table``, and vice versa.

It contains following fields:

- ``from: Dataset`` - symlink starting point.
- ``to: Dataset`` - symlink end point.
- ``type: Enum`` - type of symlink. these types are supported:

  - ``METASTORE`` - from HDFS location to Hive table in metastore.
  - ``WAREHOUSE`` - from Hive table to HDFS/S3 location.

.. note::

    Currently, OpenLineage sends only symlinks ``HDFS location → Hive table`` which `do not exist in the real world <https://github.com/OpenLineage/OpenLineage/issues/2718#issuecomment-2134746258>`_.
    Message consumer automatically adds a reverse symlink ``Hive table → HDFS location`` to simplify building lineage graph, but this is temporary solution.

.. image:: dataset_symlinks.png

Parent Relation
~~~~~~~~~~~~~~~

Relation between child run/operation and its parent. For example:

- Spark applicationName is parent for all its runs (applicationId).
- Spark applicationId is parent for all its Spark job or Spark execution.
- Airflow DAG is parent of Airflow task.
- Airflow Task Instance triggered a Spark applicationId, dbt run, and so on.

It contains following fields:

- ``from: Job | Run`` - parent entity.
- ``to: Run | Operation`` - child entity.

.. image:: parent.png

Input relation
~~~~~~~~~~~~~~

Relation Dataset → Operation, describing the process of reading some data from specific table/folder by specific operation.

It is also possible to aggregate all inputs of specific Dataset → Run, Dataset → Job or Dataset -> Dataset by adjusting  interaction ``granularity`` option of Lineage graph.

It contains following fields:

- ``from: Dataset`` - data source.
- ``to: Operation | Run | Job | Dataset`` - data target.
- ``num_rows: int | None`` - number of rows read from dataset. For ``granularity=JOB|RUN`` it is a sum of all read rows from this dataset. For ``granularity=DATASET`` always ``None``.
- ``num_bytes: int | None`` - number of bytes read from dataset. For ``granularity=JOB|RUN`` it is a sum of all read bytes from this dataset. For ``granularity=DATASET`` always ``None``.
- ``num_files: int | None`` - number of files read from dataset. For ``granularity=JOB|RUN`` it is a sum of all read files from this dataset. For ``granularity=DATASET`` always ``None``.

.. image:: input.png

Output relation
~~~~~~~~~~~~~~~

Relation Operation → Dataset, describing the process of writing some data to specific table/folder by specific Spark command, or table/folder metadata changes.

It is also possible to aggregate all outputs of specific Run → Dataset or Job → Dataset combination, by adjusting ``granularity`` option of Lineage graph.

It contains following fields:

- ``from: Operation | Run | Job`` - output source.
- ``to: Dataset`` - output target.
- ``types: list[Enum]`` - type of output. these types are supported:

  - ``CREATE``
  - ``ALTER``
  - ``RENAME``
  - ``APPEND``
  - ``OVERWRITE``
  - ``DROP``
  - ``TRUNCATE``

  For ``granularity=JOB|RUN`` it is a combination of all output types for this dataset.

- ``num_rows: int | None`` - number of rows written from dataset. For ``granularity=JOB|RUN`` it is a sum of all written rows to this dataset.
- ``num_bytes: int | None`` - number of bytes written from dataset. For ``granularity=JOB|RUN`` it is a sum of all written bytes to this dataset.
- ``num_files: int | None`` - number of files written from dataset. For ``granularity=JOB|RUN`` it is a sum of all written files to this dataset.

.. image:: output.png

Direct Column Lineage relation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Relation Dataset columns → Dataset columns, describing how each target dataset column is related to some source dataset columns.

- ``from: Dataset`` - source dataset.
- ``to: Dataset`` - target dataset.
- ``fields: dict[str, list[SourceColumn]]`` - mapping between target column name and source columns, where ``SourceColumn`` is:

  - ``field: str`` - source column name
  - ``types: list[Enum]`` - types of transformation applied to source column. Supported types are:

    - ``IDENTITY`` - column is used as-is, e.g. ``SELECT source_column AS target_column``
    - ``TRANSFORMATION`` - some non-masking function is applied to column value, e.g. ``SELECT source_column || '_suffix' AS target_column``
    - ``TRANSFORMATION_MASKING`` - some masking function is applied to column value, e.g. ``SELECT hash(source_column) AS target_column``
    - ``AGGREGATION`` - some non-masking aggregation function is applied to column value, e.g. ``SELECT max(source_column) AS target_column``
    - ``AGGREGATION_MASKING`` - some masking aggregation function is applied to column value, e.g. ``SELECT count(DISTINCT source_column) AS target_column``
    - ``UNKNOWN`` - some unknown transformation type.

.. image:: direct_column_lineage.png

Indirect Column Lineage relation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Relation Dataset columns → Dataset, describing how the entire target dataset is related to some source dataset columns.

- ``from: Dataset`` - source dataset.
- ``to: Dataset`` - target dataset.
- ``fields: list[Column]`` - list of source columns, where ``SourceColumn`` is:

  - ``field: str`` - source column name
  - ``types: list[Enum]`` - types of transformation applied to source column. Supported types are:

    - ``FILTER`` - column is used in ``WHERE`` clause, e.g. ``SELECT * WHERE source_column = 'abc'``
    - ``JOIN`` - column is used in JOIN clause, e.g. ``SELECT * FROM source_dataset1 JOIN source_dataset2 ON source_dataset1.id = source_dataset2.id``
    - ``GROUP_BY`` - column is used in ``GROUP BY`` clause, e.g. ``SELECT source_column, count(*) FROM source_dataset GROUP BY source_column``
    - ``SORT`` - column is used in ``ORDER BY`` clause, e.g. ``SELECT * FROM source_dataset ORDER BY source_column``
    - ``WINDOW`` - column is used in ``WINDOW`` clause, e.g. ``SELECT max(*) OVER (source_column) AS target_column``
    - ``CONDITIONAL`` - column is used in ``CASE`` or ``IF`` clause, e.g. ``SELECT CASE source_column THEN 1 WHEN 'abc' ELSE 'cde' END AS target_column``
    - ``UNKNOWN`` - some unknown transformation type.

.. image:: indirect_column_lineage.png
