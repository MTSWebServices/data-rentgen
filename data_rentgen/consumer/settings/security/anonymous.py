# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Literal

from data_rentgen.consumer.settings.security.base import KafkaSecurityBaseSettings


class KafkaSecurityAnonymousSettings(KafkaSecurityBaseSettings):
    """Kafka anonymous auth settings.

    Examples
    --------

    ```bash
    DATA_RENTGEN__KAFKA__SECURITY__TYPE=None
    ```
    """

    type: Literal[None] = None  # noqa: PYI061
