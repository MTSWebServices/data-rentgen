# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field

from data_rentgen.server.settings.auth.jwt import JWTSettings


class DummyAuthProviderSettings(BaseModel):
    """Settings for DummyAuthProvider.

    Examples
    --------

    ```bash
    DATA_RENTGEN__AUTH__PROVIDER=data_rentgen.server.providers.auth.dummy_provider.DummyAuthProvider
    DATA_RENTGEN__AUTH__ACCESS_KEY__SECRET_KEY=secret
    ```
    """

    access_token: JWTSettings = Field(description="Access-token related settings")
