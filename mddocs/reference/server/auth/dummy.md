# Dummy Auth provider { #auth-server-dummy }

## Description

This auth provider allows to sign-in with any username and password, and and then issues an access token.

After successful auth, username is saved to backend database.

## Interaction schema

```plantuml title="Interaction schema"

        @startuml
            title DummyAuthProvider
            participant "Client"
            participant "Backend"

            == POST v1/auth/token ==

            activate "Client"
            alt Successful case
                "Client" -> "Backend" ++ : login + password
                "Backend" --> "Backend" : Password is completely ignored
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" -> "Backend" : Create user if not exist
                "Backend" -[#green]> "Client" -- : Generate and return access_token

            else User is blocked
                "Client" -> "Backend" ++ : login + password
                "Backend" --> "Backend" : Password is completely ignored
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" x-[#red]> "Client" -- : 401 Unauthorized

            else User is deleted
                "Client" -> "Backend" ++ : login + password
                "Backend" --> "Backend" : Password is completely ignored
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" x-[#red]> "Client" -- : 404 Not found
            end

            == GET v1/datasets ==

            alt Successful case
                "Client" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Validate token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" -> "Backend" : Get data
                "Backend" -[#green]> "Client" -- : Return data

            else Token is expired
                "Client" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Validate token
                "Backend" x-[#red]> "Client" -- : 401 Unauthorized

            else User is not found
                "Client" -> "Backend" ++ : access_token
                "Backend" --> "Backend" : Validate token
                "Backend" --> "Backend" : Check user in internal backend database
                "Backend" x-[#red]> "Client" -- : 404 Not found
            end

            deactivate "Client"
        @enduml
```

## Configuration

::: data_rentgen.server.settings.auth.dummy.DummyAuthProviderSettings

::: data_rentgen.server.settings.auth.jwt.JWTSettings
