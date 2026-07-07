# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import textwrap

from pydantic import BaseModel, Field, SecretStr


class JWTSettings(BaseModel):
    """Settings related to JWT tokens.

    Examples
    --------

    ```bash
    DATA_RENTGEN__AUTH__ACCESS_KEY__SECRET_KEY=somesecret
    DATA_RENTGEN__AUTH__ACCESS_KEY__EXPIRE_SECONDS=3600  # 1 hour
    ```
    """

    secret_key: SecretStr = Field(
        description=textwrap.dedent(
            """
            Secret key for signing JWT tokens.

            Can be any string. It is recommended to generate random value for every application instance, e.g.:

            ```shell
            pwgen 32 1
            ```
            """,
        ),
    )
    security_algorithm: str = Field(
        default="HS256",
        description=textwrap.dedent(
            """
            Algorithm used for signing JWT tokens.

            See [pyjwt](https://pyjwt.readthedocs.io/en/latest/algorithms.html)
            documentation.
            """,
        ),
    )
    expire_seconds: int = Field(
        default=10 * 60 * 60,
        description="Token expiration time, in seconds",
    )
