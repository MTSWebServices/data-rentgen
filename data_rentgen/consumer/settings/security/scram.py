# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from faststream.security import SASLScram256, SASLScram512
from pydantic import Field, SecretStr

from data_rentgen.consumer.settings.security.base import KafkaSecurityBaseSettings


class KafkaSecurityScram256Settings(KafkaSecurityBaseSettings):
    """Kafka SCRAM-SHA-256 auth settings.

    Examples
    --------

    ```bash
    DATA_RENTGEN__KAFKA__SECURITY__TYPE=SCRAM-SHA-256
    DATA_RENTGEN__KAFKA__SECURITY__USER=dummy
    DATA_RENTGEN__KAFKA__SECURITY__PASSWORD=changeme
    ```
    """

    type: Literal["SCRAM-SHA-256"] = "SCRAM-SHA-256"
    user: str = Field(description="Kafka security username")
    password: SecretStr = Field(description="Kafka security password")

    def to_security(self):
        return SASLScram256(
            self.user,
            self.password.get_secret_value(),
        )


class KafkaSecurityScram512Settings(KafkaSecurityBaseSettings):
    """Kafka SCRAM-SHA-512 auth settings.

    Examples
    --------

    ```bash
    DATA_RENTGEN__KAFKA__SECURITY__TYPE=SCRAM-SHA-512
    DATA_RENTGEN__KAFKA__SECURITY__USER=dummy
    DATA_RENTGEN__KAFKA__SECURITY__PASSWORD=changeme
    ```
    """

    type: Literal["SCRAM-SHA-512"] = "SCRAM-SHA-512"
    user: str = Field(description="Kafka security username")
    password: SecretStr = Field(description="Kafka security password")

    def to_security(self):
        return SASLScram512(
            self.user,
            self.password.get_secret_value(),
        )
