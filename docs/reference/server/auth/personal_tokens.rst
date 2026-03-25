.. _auth-server-personal-tokens:

Personal Tokens
===============

Description
-----------

This auth schema (not actually a AuthProvider) allows to access API endpoints using :ref:`personal-tokens`.
If enabled, it has higher priority than AuthProvider.

.. note::

    Some endpoints, like creating/refreshing Personal Tokens, cannot be used with this auth type,
    as they require human interaction.

Interaction schema
------------------

.. dropdown:: Interaction schema

    .. plantuml::

        @startuml
            title PersonalTokensAuthProvider
            participant "Client"
            participant "Backend"
            participant "Database"

            == POST v1/personal-tokens ==

            activate "Client"
            "Client" -> "Backend" ++ : create new Personal token
            "Backend" -[#green]> "Client" -- : Generate and return personal_token

            == GET v1/datasets ==

            alt Successful case (first request)
                "Client" -> "Backend" ++ : Authorization Bearer personal_token
                "Backend" -[#green]> "Backend" : Validate token
                "Backend" -[#red]x "Backend" : Get token info from in-memory cache
                "Backend" --> "Database" ++ : Fetch token info
                "Database" --> "Backend" : Return token info
                "Backend" -[#green]> "Backend" : Cache token
                "Backend" --> "Database" : Fetch data
                "Database" -[#green]> "Backend" -- : Return data
                "Backend" -[#green]> "Client" -- : Return data

            else Successful case (second request)
                "Client" -> "Backend" ++ : Authorization Bearer personal_token
                "Backend" -[#green]> "Backend" : Validate token
                "Backend" -[#green]> "Backend" : Get token info from in-memory cache
                "Backend" --> "Database" ++ : Fetch data
                "Database" -[#green]> "Backend" -- : Return data
                "Backend" -[#green]> "Client" -- : Return data

            else Token is expired
                "Client" -> "Backend" ++ : Authorization Bearer personal_token
                "Backend" -[#red]x "Backend" : Validate token
                "Backend" x-[#red]> "Client" -- : 401 Unauthorized

            else Token was revoked
                "Client" -> "Backend" ++ : Authorization Bearer personal_token
                "Backend" -[#green]> "Backend" : Validate token
                "Backend" -[#red]x "Backend" : Get token info from in-memory cache
                "Backend" --> "Database" : Fetch token info
                "Database" x-[#red]> "Backend" -- : No active token in database
                "Backend" x-[#red]> "Client" -- : 401 Unauthorized
            end

            deactivate "Client"
        @enduml

Basic Configuration
-------------------

.. autopydantic_model:: data_rentgen.server.settings.auth.personal_token.PersonalTokenSettings


