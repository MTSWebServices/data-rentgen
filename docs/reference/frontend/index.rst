.. _frontend:

Frontend
========

Data.Rentgen provides a `Frontend (UI) <https://github.com/MTSWebServices/data-rentgen-ui>`_ based on `ReactAdmin <https://marmelab.com/react-admin/>`_ and `ReactFlow <https://reactflow.dev/>`_,
providing users the ability to navigate entities and build lineage graph.

Install & run
-------------

With Docker
~~~~~~~~~~~

* Install `Docker <https://docs.docker.com/engine/install/>`_
* Install `docker-compose <https://github.com/docker/compose/releases/>`_

* Run the following command:

  .. code:: console

    $ docker compose --profile frontend up -d --wait

  ``docker-compose`` will download Data.Rentgen UI image, create containers, and then start them.

  Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

  .. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 140-151

  .. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 36-37

* After frontend is started and ready, open http://localhost:3000.

See also
--------

.. toctree::
    :maxdepth: 1

    configuration
