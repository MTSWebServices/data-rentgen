.. _overview-setup-dbt:

dbt integration
===============

Using `OpenLineage integration with dbt <https://openlineage.io/docs/integrations/dbt>`_.

Requirements
------------

* `dbt <https://www.getdbt.com/>`_ 1.3 or higher
* OpenLineage 1.19.0 or higher, recommended 1.40.1+
* Running :ref:`message-broker`
* (Optional) :ref:`http2kafka`

Limitations
-----------

* Currently there is no way to pass dataset tags, `see issue <https://github.com/OpenLineage/OpenLineage/issues/3500>`_.

Entity mapping
--------------

* dbt project → Data.Rentgen Job
* dbt run → Data.Rentgen Run
* dbt model, snapshot, sql, test → Data.Rentgen Operation

Install
-------

.. tabs::

  .. code-tab:: console KafkaTransport

    $ pip install "openlineage-dbt>=1.40.1" "openlineage-python[kafka]>=1.40.1" zstd

  .. code-tab:: console HttpTransport (requires HTTP2Kafka)

    $ pip install "openlineage-dbt>=1.40.1"

Setup
-----

* Create ``openlineage.yml`` file with content like:

  .. tabs::

    .. code-tab:: yaml KafkaTransport

      transport:
          type: kafka
          topic: input.runs
          config:
              # should be accessible from host
              bootstrap.servers: localhost:9093
              security.protocol: SASL_PLAINTEXT
              sasl.mechanism: SCRAM-SHA-256
              # Kafka auth credentials
              sasl.username: data_rentgen
              sasl.password: changeme
              compression.type: zstd
              acks: all

    .. code-tab:: yaml HttpTransport (requires HTTP2Kafka)

      transport:
          # "type: http" for OpenLineage below 1.35.0
          type: async_http
          # http2kafka URL, should be accessible from host
          url: http://localhost:8002
          endpoint: /v1/openlineage
          compression: gzip
          auth:
              type: api_key
              # create a PersonalToken, and pass it here
              apiKey: personal_token_AAAAAAAAAAAA.BBBBBBBBBBBBBBBBBBBBBBB.CCCCCCCCCCCCCCCCCCCCC

* Set environment variables:

  .. code:: ini

      OPENLINEAGE_NAMESPACE=local://dbt.host.name
      OPENLINEAGE_CONFIG=/path/to/openlineage.yml

Collect and send lineage
------------------------

Replace ``dbt`` CLI commands:

.. code:: shell

    $ dbt run myproject
    $ dbt test myproject

with ``dbt-ol`` CLI:

.. code:: shell

    $ dbt-ol run myproject
    $ dbt-ol test myproject

Lineage will be send to Data.Rentgen automatically by OpenLineage integration.

See results
-----------

Browse frontend page `Jobs <http://localhost:3000/jobs>`_ to see what information was extracted by OpenLineage & DataRentgen

Job list page
~~~~~~~~~~~~~

.. image:: ./job_list.png

Job details page
~~~~~~~~~~~~~~~~

.. image:: ./job_details.png

Job-level lineage
~~~~~~~~~~~~~~~~~

.. image:: ./job_lineage.png

Run details
~~~~~~~~~~~

.. image:: ./run_details.png

Run lineage
~~~~~~~~~~~

.. image:: ./run_lineage.png

Operation details
~~~~~~~~~~~~~~~~~

.. image:: ./operation_details.png

Operation lineage
~~~~~~~~~~~~~~~~~

.. image:: ./operation_lineage.png

Extra configuration
-------------------

Collecting model tags
~~~~~~~~~~~~~~~~~~~~~

By default, following job tags are created:

- ``dbt.version``
- ``openlineage_adapter.version``
- ``openlineage_client.version`` (using OpenLineage client 1.38.0+)

It is possible to provide custom tags via model config:

.. code-block:: yaml
    :caption: dbt_project.yaml

    models:
    jaffle_shop:
        materialized: table
        staging:
            materialized: view
        +tags:
            - environment:production
            - layer:bronze

Binding Airflow Task with Spark application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If OpenLineage event contains `Parent Run facet <https://openlineage.io/docs/spec/facets/run-facets/parent_run/>`_,
DataRentgen can use this information to bind dbt run to the run it was triggered by, e.g. Airflow task:

.. image:: ../airflow/job_hierarchy.png

To fill up this facet, it is required to:

* Setup OpenLineage integration for dbt
* Setup :ref:`OpenLineage integration for Airflow <overview-setup-airflow>`
* Pass parent Run info from Airflow to dbt by using `Airflow macros <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/macros.html#lineage-job-run-macros>`_:

  .. tabs::

    .. code-tab:: py BashOperator

        from airflow.providers.standard.operators.bash import BashOperator

        task = BashOperator(
            task_id="dbt_run_task",
            cwd="/path/to/project",
            bash_command="dbt-ol run",
            append_env=True,
            env={
                # Pass parent Run info from Airflow to Spark
                "OPENLINEAGE_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}",
                # For apache-airflow-providers-openlineage 2.4.0 or above
                "OPENLINEAGE_ROOT_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}",
            }
        )

    .. code-tab:: py SSHOperator

        from airflow.providers.ssh.operators.ssh import SSHOperator

        task = SSHOperator(
            task_id="dbt_run_task",
            ssh_conn_id="some_host",
            command="cd /path/to/project && dbt-ol run",
            environment={
                "OPENLINEAGE_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}",
                # For apache-airflow-providers-openlineage 2.4.0 or above
                "OPENLINEAGE_ROOT_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}",
            }
        )

    .. code-tab:: py KubernetesPodOperator

        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

        task = SSHOperator(
            task_id="dbt_run_task",
            cmds=["bash", "-cx"],
            arguments=["cd /path/to/project && dbt-ol run"],
            env_vars={
                "OPENLINEAGE_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}",
                # For apache-airflow-providers-openlineage 2.4.0 or above
                "OPENLINEAGE_ROOT_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}",
            }
        )
