# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field, SecretStr


class KeycloakSettings(BaseModel):
    server_url: str = Field(..., description="Keycloak server URL")
    client_id: str = Field(..., description="Keycloak client ID")
    realm_name: str = Field(..., description="Keycloak realm name")
    client_secret: SecretStr = Field(..., description="Keycloak client secret")
    redirect_uri: str = Field(..., description="Redirect URI")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")
    scope: str = Field("openid", description="Keycloak scope")


class KeycloakAuthProviderSettings(BaseModel):
    """Settings related to Keycloak interaction."

    Examples
    --------

    ```bash
    DATA_RENTGEN__AUTH__PROVIDER=data_rentgen.server.providers.auth.keycloak_provider.KeycloakAuthProvider
    DATA_RENTGEN__AUTH__KEYCLOAK__SERVER_URL=http://keycloak:8080
    DATA_RENTGEN__AUTH__KEYCLOAK__REDIRECT_URI=http://localhost:8000/auth-callback
    DATA_RENTGEN__AUTH__KEYCLOAK__REALM_NAME=fastapi_realm
    DATA_RENTGEN__AUTH__KEYCLOAK__CLIENT_ID=fastapi_client
    DATA_RENTGEN__AUTH__KEYCLOAK__CLIENT_SECRET=generated_by_keycloak
    DATA_RENTGEN__AUTH__KEYCLOAK__SCOPE=email
    DATA_RENTGEN__AUTH__KEYCLOAK__VERIFY_SSL=False
    ```
    """

    keycloak: KeycloakSettings = Field(
        description="Keycloak settings",
    )
