# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Literal

from data_rentgen.consumer.settings.security.base import KafkaSecurityBaseSettings


class KafkaSecurityAnonymousSettings(KafkaSecurityBaseSettings):
    """Kafka anonymous auth settings.

    Examples
    --------

    ```yaml title="config.yml"
    kafka:
      security:
        type: null
    ```
    """

    type: Literal[None] = None  # noqa: PYI061
