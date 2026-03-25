.. _auth-server:

Authentication and Authorization
================================

Overview
--------

To access the service's endpoints, a client must authenticate. The service provides several options for authentication.

Currently, the service does not implement a role-based model, and all users have the same level of permissions.

Authentication is implemented via middleware as follows: before each endpoint call, the ``get_user()`` function is invoked.
This function attempts to retrieve the username from the provided token.

Data.Rentgen supports different auth provider implementations. You can change implementation via settings:

.. autopydantic_model:: data_rentgen.server.settings.auth.AuthSettings

Right now service has two scenarios for authentication:

- `Dummy(JWT Tokens) <https://jwt.io/>`_ a lightweight option for testing and development.
- `Keycloak authentication <https://www.keycloak.org/>`_ recommended option. Integrates with Keycloak for token-based authentication.

.. toctree::
    :maxdepth: 1
    :caption: Authentication Providers

    dummy
    keycloak
    personal_tokens

.. toctree::
    :maxdepth: 1
    :caption: For developers

    custom
