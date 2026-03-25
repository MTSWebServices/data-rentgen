.. _auth-server-keycloak:

Keycloak Provider
=================

Description
-----------

Using `OpenID Connect (OIDC) auth with Keycloak <https://www.keycloak.org/securing-apps/oidc-layers>`_.

Unlike other auth providers, access token + refresh token returned from Keycloak are stored in http-only encrypted cookie,
so JavaScript on Frontend have no access to it.

.. warning::

    This provider requires human interaction with browser and Keycloak login page, and not suitable for API access from
    scripts.

Interaction schema
------------------

.. dropdown:: Interaction schema

    .. plantuml::

        @startuml
            title DummyAuthProvider
            participant "Frontend"
            participant "Backend"
            participant "Keycloak"

            == GET /v1/datasets ==

            activate "Frontend"

            alt No session cookie
                "Frontend" -> "Backend" ++ : Cookie none
                "Backend" x-[#red]> "Frontend": 401 with redirect url in 'details' response field
                "Frontend" -> "Keycloak" ++ : Redirect user to Keycloak login page

                "Keycloak" -> "Frontend" : Redirect to to Frontend '/callback'

                "Frontend" -> "Backend" : Send request to Backend '/v1/auth/callback'

                "Backend" -> "Keycloak" : Exchange authorization code to access+refresh token pair
                "Keycloak" --> "Backend" -- : Return token pair
                "Backend" --> "Frontend" -- : Set session cookie

            else Login failed
                "Frontend" -> "Backend" ++ : Cookie none
                "Backend" x-[#red]> "Frontend" --: 401 with redirect url in 'details' response field
                "Frontend" -> "Keycloak" ++ : Redirect user to Keycloak login page
                "Keycloak" x-[#red]> "Frontend" -- : Wrong credentials or user does not exist
            end

            alt Successful case
                "Frontend" -> "Backend" ++ : Cookie session_cookie
                "Backend" -[#green]> "Backend" : Decrypt cookie
                "Backend" --> "Keycloak" ++ : Decode access_token
                "Keycloak" -[#green]> "Backend" -- : Successful
                "Backend" -> "Database" ++ : Fetch user info
                "Database" -> "Backend" : Return user into
                "Backend" -> "Database" : Fetch data
                "Database" -> "Backend" -- : Return data
                "Backend" -[#green]> "Frontend" -- : Return data

            else Access token is expired or malformed
                "Frontend" -> "Backend" ++ : Cookie session_cookie
                "Backend" -[#green]> "Backend" : Decrypt cookie
                "Backend" --> "Keycloak" ++ : Decode access_token
                "Keycloak" x-[#red]> "Backend" : Expired token
                "Backend" --> "Keycloak" : Exchange refresh token to new access_token
                "Keycloak" --> "Backend" -- : Successful
                "Backend" -> "Database" ++ : Check user in internal backend database
                "Database" -> "Backend" : Return user
                "Backend" -> "Database" : Fetch data
                "Database" -> "Backend" -- : Return data
                "Backend" -[#green]> "Frontend" -- : Return data

            else Access token is expired, refresh token is expired or malformed
                "Frontend" -> "Backend" ++ : Cookie session_cookie
                "Backend" -[#yellow]> "Backend" : Decrypt cookie
                "Backend" --> "Keycloak" ++ : Decode access_token
                "Keycloak" x-[#red]> "Backend" : Expired token
                "Backend" --> "Keycloak" : Exchange refresh token to new access_token
                "Keycloak" x-[#red]> "Backend" -- : Bad refresh_token
                "Backend" x-[#red]> "Frontend" -- : RedirectResponse can't refresh

            else Bad response from Keycloak
                "Frontend" -> "Backend" ++ : Cookie session_cookie
                "Backend" -[#green]> "Backend" : Decrypt cookie
                "Backend" --> "Keycloak" ++ : Decode access_token
                "Keycloak" x-[#red]> "Backend" -- : Error
                "Backend" x-[#red]> "Frontend" -- : 307 Authorization error
            end

            deactivate "Frontend"
        @enduml


Basic Configuration
-------------------

.. autopydantic_model:: data_rentgen.server.settings.auth.keycloak.KeycloakAuthProviderSettings
.. autopydantic_model:: data_rentgen.server.settings.auth.keycloak.KeycloakSettings
