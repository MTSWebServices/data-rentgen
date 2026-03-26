# Keycloak Provider { #auth-server-keycloak }

## Description

Keycloak auth provider uses [python-keycloak](https://pypi.org/project/python-keycloak/) library to interact with Keycloak server. During the authentication process,
KeycloakAuthProvider redirects user to Keycloak authentication page.

After successful authentication, Keycloak redirects user back to Data.Rentgen with authorization code.
Then KeycloakAuthProvider exchanges authorization code for an access token and uses it to get user information from Keycloak server.
If user is not found in Data.Rentgen database, KeycloakAuthProvider creates it. Finally, KeycloakAuthProvider returns user with access token.

## Interaction schema

```plantuml title="Interaction schema"

        @startuml
            title DummyAuthProvider
            participant "Frontend"
            participant "Backend"
            participant "Keycloak"

            == Frontend Authentication at Keycloak ==

            Frontend -> Backend : Request endpoint with authentication (/v1/locations)

            Backend x-[#red]> Frontend: 401 with redirect url in 'details' response field

            Frontend -> Keycloak : Redirect user to Keycloak login page

            alt Successful login
                Frontend --> Keycloak : Log in with login and password
            else Login failed
                Keycloak x-[#red]> Frontend -- : Display error (401 Unauthorized)
            end

            Keycloak -> Frontend : Callback to Frontend /callback which is proxy between Keycloak and Backend

            Frontend -> Backend : Send request to Backend '/v1/auth/callback'

            Backend -> Keycloak : Check original 'state' and exchange code for token's
            Keycloak --> Backend : Return token's
            Backend --> Frontend : Set token's in user's browser in cookies

            Frontend --> Backend : Request to /v1/locations with session cookies
            Backend -> Backend : Get user info from token and check user in internal backend database
            Backend -> Backend : Create user in internal backend database if not exist
            Backend -[#green]> Frontend -- : Return requested data


            == GET v1/datasets ==


            alt Successful case
                "Frontend" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Validate token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" -> "Backend" : Get data
                "Backend" -[#green]> "Frontend" -- : Return data

            else Token is expired (Successful case)
                "Frontend" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Validate token
                "Backend" -[#yellow]> "Backend" : Token is expired
                "Backend" --> "Keycloak" : Try to refresh token
                "Backend" --> "Backend" : Validate new token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" -> "Backend" : Get data
                "Backend" -[#green]> "Frontend" -- : Return data

            else Create new User
                "Frontend" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Validate token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" --> "Backend" : Create new user
                "Backend" -> "Backend" : Get data
                "Backend" -[#green]> "Frontend" -- : Return data

            else Token is expired and bad refresh token
                "Frontend" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Validate token
                "Backend" -[#yellow]> "Backend" : Token is expired
                "Backend" --> "Keycloak" : Try to refresh token
                "Backend" x-[#red]> "Frontend" -- : RedirectResponse can't refresh

            else Bad Token payload
                "Frontend" -> "Backend" ++ : access_token, refresh_token
                "Backend" --> "Backend" : Validate token
                "Backend" x-[#red]> "Frontend" -- : 307 Authorization error

            end

            deactivate "Frontend"
        @enduml

```

## Basic Configuration

::: data_rentgen.server.settings.auth.keycloak.KeycloakAuthProviderSettings

::: data_rentgen.server.settings.auth.keycloak.KeycloakSettings
