# Dummy Auth provider { #auth-server-dummy }

## Description

This auth provider allows to sign-in with any username and password, and and then issues an access token.

After successful auth, username is saved to backend database.

## Interaction schema

```mermaid
sequenceDiagram
participant "Client"
participant "Backend"

activate "Client"
alt Successful case
"Client" ->> "Backend"  : login + password
"Backend" ->> "Backend" : Password is completely ignored
"Backend" ->> "Backend" : Check user in internal backend database
"Backend" ->> "Backend" : Create user if not exist
"Backend" ->> "Client"  : Generate and return access_token

else User is blocked
"Client" ->> "Backend"  : login + password
"Backend" ->> "Backend" : Password is completely ignored
"Backend" ->> "Backend" : Check user in internal backend database
"Backend" --x "Client"  : 401 Unauthorized

else User is deleted
"Client" ->> "Backend"  : login + password
"Backend" ->> "Backend" : Password is completely ignored
"Backend" ->> "Backend" : Check user in internal backend database
"Backend" --x "Client"  : 404 Not found
end

alt Successful case
"Client" ->> "Backend"  : access_token
"Backend" ->> "Backend" : Validate token
"Backend" ->> "Backend" : Check user in internal backend database
"Backend" ->> "Backend" : Get data
"Backend" ->> "Client"  : Return data

else Token is expired
"Client" ->> "Backend"  : access_token
"Backend" ->> "Backend" : Validate token
"Backend" --x "Client"  : 401 Unauthorized

else User is not found
"Client" ->> "Backend"  : access_token
"Backend" ->> "Backend" : Validate token
"Backend" ->> "Backend" : Check user in internal backend database
"Backend" --x "Client"  : 404 Not found
end

deactivate "Client"
```

## Configuration

::: data_rentgen.server.settings.auth.dummy.DummyAuthProviderSettings

::: data_rentgen.server.settings.auth.jwt.JWTSettings
