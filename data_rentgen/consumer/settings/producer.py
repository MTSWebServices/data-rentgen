# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from pydantic import BaseModel, Field, NonNegativeInt


class ProducerSettings(BaseModel):
    """Data.Rentgen producer-specific settings.

    These options are passed directly to
    [AIOKafkaProducer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaProducer).

    Examples
    --------

    ```yaml title="config.yml"
    producer:
      main_topic: input.runs
      malformed_topic: input.runs__malformed
    ```
    """

    main_topic: str = Field(
        default="input.runs",
        description="Topic to publish messages to.",
    )

    malformed_topic: str = Field(
        default="input.runs__malformed",
        description="Topic to publish malformed messages to.",
    )
    acks: NonNegativeInt | Literal["all"] = Field(
        default="all",
        description="Number of required acknowledgments.",
    )
    max_batch_size: int = Field(
        default=16 * 1024,
        description="Maximum size of buffered data per partition.",
    )
    max_request_size: int = Field(
        default=5 * 1024 * 1024,
        description="Maximum request size in bytes.",
    )
