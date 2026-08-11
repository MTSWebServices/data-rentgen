# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, ConfigDict, Field, ImportString, field_validator

from data_rentgen.server.providers.auth.base_provider import AuthProvider
from data_rentgen.server.settings.auth.personal_token import PersonalTokenSettings


class AuthSettings(BaseModel):
    """Authorization-related settings.

    Here you can set auth provider class along with its options.

    Examples
    --------

    ```yaml title="config.yml"
    auth:
      provider: data_rentgen.server.providers.auth.dummy_provider.DummyAuthProvider
      # pass access_key.secret_key = "secret" to DummyAuthProviderSettings
      access_key:
        secret_key: secret
    ```
    """

    provider: ImportString = Field(  # type: ignore[assignment]
        default="data_rentgen.server.providers.auth.dummy_provider.DummyAuthProvider",
        description="Full name of auth provider class",
        validate_default=True,
    )
    # switching between auth provider class doesn't change how Personal Tokens are treated,
    # so these are in separate settings
    personal_tokens: PersonalTokenSettings = Field(
        default_factory=PersonalTokenSettings,
        description="Settings for generating Personal Access Tokens",
    )

    model_config = ConfigDict(extra="allow")

    @field_validator("provider", mode="after")
    @classmethod
    def _validate_provider(cls, value: type) -> type[AuthProvider]:
        if not issubclass(value, AuthProvider):
            msg = f"Class {value} is not a subclass of {AuthProvider}"
            raise TypeError(msg)
        return value

    # prevent leaking provider secrets
    def __repr_args__(self):
        return [("provider", self.provider)]
