.. _auth-server-dummy:

Dummy Auth provider
===================

Description
-----------

This auth provider allows to sign-in with any username and password, and then issues an access token.

.. warning::

    This provider is used only for demo and testing purposes. It should **NEVER** be used on production!

Interaction schema
------------------

.. dropdown:: Interaction schema

    .. plantuml::

        @startuml
            title DummyAuthProvider
            participant "Client"
            participant "Backend"
            participant "Database"

            == POST v1/auth/token ==

            activate "Client"
            alt New user
                "Client" -> "Backend" ++ : Pass login + password (ignored)
                "Backend" --> "Database" ++ : Fetch user info
                "Database" x-[#red]> "Backend" : No such user
                "Backend" --> "Database" : Create new user
                "Database" -[#green]> "Backend" -- : Return user info
                "Backend" -[#green]> "Client" -- : Generate and return access_token

            else Existing user
                "Client" -> "Backend" ++ : Pass login + password (ignored)
                "Backend" --> "Database" ++ : Fetch user info
                "Database" -[#green]> "Backend" -- : Return user info
                "Backend" -[#green]> "Client" -- : Generate and return access_token
            end

            == GET v1/datasets ==

            alt Successful case
                "Client" -> "Backend" ++ : Authorization Bearer access_token
                "Backend" -[#green]> "Backend" : Validate token
                "Backend" --> "Database" ++ : Get data
                "Database" -[#green]> "Backend" -- : Return data
                "Backend" -[#green]> "Client" -- : Return data

            else Token is expired or malformed
                "Client" -> "Backend" ++ : Authorization Bearer access_token
                "Backend" -[#red]x "Backend" : Validate token
                "Backend" x-[#red]> "Client" -- : 401 Unauthorized
            end

            deactivate "Client"
        @enduml

Configuration
-------------

.. autopydantic_model:: data_rentgen.server.settings.auth.dummy.DummyAuthProviderSettings
.. autopydantic_model:: data_rentgen.server.settings.auth.jwt.JWTSettings
