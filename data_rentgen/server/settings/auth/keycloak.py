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

    ```yaml title="config.yml"
    auth:
      provider: data_rentgen.server.providers.auth.keycloak_provider.KeycloakAuthProvider
      keycloak:
        server_url: http://keycloak:8080
        redirect_uri: http://localhost:8000/auth-callback
        realm_name: fastapi_realm
        client_id: fastapi_client
        client_secret: generated_by_keycloak
        scope: email
        verify_ssl: false
    ```
    """

    keycloak: KeycloakSettings = Field(
        description="Keycloak settings",
    )
