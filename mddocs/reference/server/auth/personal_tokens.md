# Personal Tokens { #auth-server-personal-tokens }

## Description

This auth schema (not actually a AuthProvider) allows to access API endpoints using [personal-tokens][personal-tokens].
If enabled, it has higher priority than AuthProvider.

!!! note
    Some endpoints, like creating/refreshing Personal Tokens, cannot be used with this auth type, as they require human interaction.

## Interaction schema

```mermaid
sequenceDiagram

title PersonalTokensAuthProvider

participant "Client"
participant "Backend"
participant "Database"

activate "Client"
"Client" -> "Backend" : create new Personal token
"Backend" --> "Client" : Generate and return personal_token

alt Successful case (first request)
"Client" -> "Backend" : Authorization Bearer personal_token
"Backend" --> "Backend" : Validate token
"Backend" --x "Backend" : Get token info from in-memory cache
"Backend" --> "Database" : Fetch token info
"Database" --> "Backend" : Return token info
"Backend" --> "Backend" : Cache token
"Backend" --> "Database" : Fetch data
"Database" --> "Backend": Return data
"Backend" --> "Client" : Return data

else Successful case (second request)
"Client" -> "Backend": Authorization Bearer personal_token
"Backend" --> "Backend" : Validate token
"Backend" --> "Backend" : Get token info from in-memory cache
"Backend" --> "Database" : Fetch data
"Database" --> "Backend": Return data
"Backend" --> "Client": Return data

else Token is expired
"Client" -> "Backend" : Authorization Bearer personal_token
"Backend" --x "Backend" : Validate token
"Backend" --x "Client" : 401 Unauthorized

else Token was revoked
"Client" -> "Backend": Authorization Bearer personal_token
"Backend" --> "Backend" : Validate token
"Backend" --x "Backend" : Get token info from in-memory cache
"Backend" --> "Database" : Fetch token info
"Database" --x "Backend" : No active token in database
"Backend" --x "Client" : 401 Unauthorized
end

deactivate "Client"
```

## Basic Configuration

::: data_rentgen.server.settings.auth.personal_token.PersonalTokenSettings
