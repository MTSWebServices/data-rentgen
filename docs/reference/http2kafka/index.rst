.. _http2kafka:

HTTP2Kafka proxy
================

Some of OpenLineage integrations support only HttpTransport, but not KafkaTransport, e.g. Trino.

Data.Rentgen HTTP → Kafka proxy is optional component which provides a simple HTTP API receiving
`OpenLineage run events <https://openlineage.io/docs/spec/object-model>`_ in JSON format and sending them to Kafka topic as is,
so they can be handled by :ref:`message-consumer` in a proper way.

.. warning::

    Due to OpenLineage limitations, HTTP2kafka can be used only with :ref:`personal-tokens`, and no other auth methods are supported.

OpenLineage HttpTransport or KafkaTransport?
--------------------------------------------

Introducing additional layer between OpenLineage and Kafka reduces performance a bit:
  * It parses all incoming events for validation and routing purposes. The larger the event, the slower the parsing.
  * HTTP/HTTPS protocol is far more complex than Kafka TCP protocol, and has much higher latency in the first place.

If some OpenLineage integration supports both HttpTransport and KafkaTransport, and Kafka doesn't use
complex authentication not supported by OpenLineage (e.g. OAUTHBEARER), always prefer KafkaTransport.

If this is not possible, HTTP2Kafka is the way to go.

Install & run
-------------

With docker
^^^^^^^^^^^

* Install `Docker <https://docs.docker.com/engine/install/>`_
* Install `docker-compose <https://github.com/docker/compose/releases/>`_

* Run the following command:

  .. code:: console

    $ docker compose --profile http2kafka up -d --wait

  ``docker-compose`` will download all necessary images, create containers, and then start the component.

  Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

  .. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 155-173

  .. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 29-34

* After component is started and ready, open http://localhost:8002/docs.

Without docker
^^^^^^^^^^^^^^

* Install Python 3.10 or above
* Setup :ref:`message-broker`
* Create virtual environment

  .. code-block:: console

      $ python -m venv /some/.venv
      $ source /some/.venv/activate

* Install ``data-rentgen`` package with following *extra* dependencies:

  .. code-block:: console

      $ pip install data-rentgen[http2kafka]

* Run http2kafka process

  .. code-block:: console

      $ python -m data_rentgen.http2kafka --host 0.0.0.0 --port 8002

  This is a thin wrapper around `uvicorn <https://www.uvicorn.org/#command-line-options>`_ cli,
  options and commands are just the same.

* After server is started and ready, open http://localhost:8002/docs.

See also
--------

.. toctree::
    :maxdepth: 1

    configuration/index
    openapi
    alternatives
