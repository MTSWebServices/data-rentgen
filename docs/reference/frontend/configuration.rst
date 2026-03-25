.. _configuration-frontend:

Frontend configuration
======================

API URL
-------

Data.Rentgen UI requires REST API to be accessible from browser. API url is set up using environment variable:

.. code:: bash

    DATA_RENTGEN__UI__API_BROWSER_URL=http://localhost:8000

If both REST API and frontend are served on the same domain (e.g. through Nginx reverse proxy), for example:

- REST API → ``/api``
- Frontend → ``/``

Then you can use relative path:

.. code:: bash

    DATA_RENTGEN__UI__API_BROWSER_URL=/api

Auth provider
-------------

By default, Data.Rentgen UI shows login page with username & password fields, designed for :ref:`auth-server-dummy`.
To show a login page for :ref:`auth-server-keycloak`, you should pass this environment variable to frontend container:

.. code:: bash

    DATA_RENTGEN__UI__AUTH_PROVIDER=keycloakAuthProvider
