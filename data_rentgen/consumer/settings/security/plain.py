# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from faststream.security import SASLPlaintext
from pydantic import Field, SecretStr

from data_rentgen.consumer.settings.security.base import KafkaSecurityBaseSettings


class KafkaSecurityPlaintextSettings(KafkaSecurityBaseSettings):
    """Kafka PLAINTEXT auth settings.

    Examples
    --------

    ```yaml title="config.yml"
    kafka:
      security:
        type: PLAINTEXT
        user: dummy
        password: changeme
    ```
    """

    type: Literal["PLAINTEXT"] = "PLAINTEXT"
    user: str = Field(description="Kafka security username")
    password: SecretStr = Field(description="Kafka security password")

    def to_security(self):
        return SASLPlaintext(
            self.user,
            self.password.get_secret_value(),
        )
