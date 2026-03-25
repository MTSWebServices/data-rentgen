.. _personal-tokens:

Personal Tokens
===============

Different :ref:`AuthProviders <auth-server>` uses different ways to authorize user - via cookie, via short-lived access token, and so on.
This is okay for navigating in UI using browser, but not so convenient for accessing API endpoints from some script.

For example, OpenLineage is usually integrated into long running ETL processes, and it cannot make additional request to ``/auth`` endpoint,
refresh access tokens, store cookies, follow redirects from Keycloak, and so on.
The only way OpenLineage can authorize in API is to use long lived tokens (``Bearer`` auth).

This is why Data.Rentgen allows to use Personal Tokens for authorization. These tokens have higher priority than AuthProvider, and can be used
with any API endpoint.

Token is valid if:

* It is not expired. Max token duration is 1 year (configurable).
* It is not revoked.

Personal tokens can be disabled in :ref:`Server config <auth-server-personal-tokens>`.

Managing personal tokens
------------------------

Create token
~~~~~~~~~~~~

Go to user menu -> ``Personal tokens``:

.. image:: user_menu.png

Click on ``Create``:

.. image:: empty_list.png

Enter token name (mandatory) and expiration date (optional, max 1 year):

.. image:: create.png

After token is created, its content will be copied to a clipboard:

.. image:: created.png

Token content looks like this:

.. code::

    personal_token_AAAAAAAAAAA.BBBBBBBBBBBBBB.CCCCCCCCCCCC

**It is important to save this value into some secret place. If token content is lost, it cannot be generated again.**

Refresh token
~~~~~~~~~~~~~

Token can be refreshed, to update its expiration date. Go to tokens list:

.. image:: user_menu.png

.. image:: list.png

And then click on ``Refresh``:

.. image:: refresh.png

The only field which can be changed here is ``Until``. It can be set manually, or be reset to use max available until date (now + 1year).

After token is refreshed, it's content will be copied to a clipboard. Please save it to a secret place, and replace old token value with a new one.

Revoke token
~~~~~~~~~~~~

Token can be revoked if it is not needed anymore or if it was leaked/compromised.

To do this, navigate to tokens list:

.. image:: user_menu.png
.. image:: list.png

Click ``Revoke`` button, and then on ``Confirm``:

.. image:: revoke.png

After token is revoked, it cannot be used anymore.

Using personal tokens
---------------------

With OpenLineage
~~~~~~~~~~~~~~~~

.. note::

    Due to OpenLineage limitations, :ref:`http2kafka` can be used only with Personal Tokens, and no other auth methods are supported.

Use `OpenLineage HTTPTransport <https://openlineage.io/docs/client/python#http-transport>`_:

.. code-block:: yaml
    :caption: openlineage.yaml

    transport:
        type: http
        url: http://localhost:8002  # http2kafka URL
        endpoint: /v1/openlineage
        compression: gzip
        auth:
            type: api_key
            # replace with you token
            apiKey: personal_token_AAAAAAAAAAAA.BBBBBBBBBBBBBBBBBBBBBBB.CCCCCCCCCCCCCCCCCCCCC

.. code-block:: ini
    :caption: spark.conf

    spark.openlineage.transport.type=http
    spark.openlineage.transport.url=http://localhost:8002  # http2kafka URL
    spark.openlineage.transport.endpoint=/v1/openlineage
    spark.openlineage.transport.compression=gzip
    spark.openlineage.transport.auth.type=api_key
    # replace with you token
    spark.openlineage.transport.auth.apiKey=personal_token_AAAAAAAAAAAA.BBBBBBBBBBBBBBBBBBBBBBB.CCCCCCCCCCCCCCCCCCCCC

With Swagger UI
~~~~~~~~~~~~~~~

Open http://localhost:8000 (API) or http://localhost:8002 (HTTP2Kafka), and click on ``Authorize`` button:

.. image:: swagger_header.png

Enter token content into ``HTTPBearer`` auth method, and click ``Authorize``

.. image:: swagger_httpbearer.png


With ``curl``
~~~~~~~~~~~~~

.. code:: bash

    curl -XGET http://localhost:8000/v1/datasets -H 'Authorization: Bearer personal_token_AAAAAAAAAAAA.BBBBBBBBBBBBBBBBBBBBBBB.CCCCCCCCCCCCCCCCCCCCC'

With ``requests``
~~~~~~~~~~~~~~~~~

.. code:: python

    import requests

    session = requests.Session()
    personal_token = "personal_token_AAAAAAAAAAAA.BBBBBBBBBBBBBBBBBBBBBBB.CCCCCCCCCCCCCCCCCCCCC"

    with requests.Session() as session:
    response = session.get(
        url="http://localhost:8000/v1/datasets",
        headers={
            "Authorization": f"Bearer {personal_token}",
        },
    )
    response.raise_for_status()
