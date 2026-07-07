# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap
from typing import Literal

from pydantic import BaseModel, ByteSize, Field, model_validator


class ConsumerSettings(BaseModel):
    """Data.Rentgen consumer-specific settings.

    These options are passed directly to
    [AIOKafkaConsumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer).

    Examples
    --------

    ```bash
    DATA_RENTGEN__CONSUMER__TOPICS_LIST=["input.runs"]
    DATA_RENTGEN__CONSUMER__MALFOMED_TOPIC="input.runs:malformed"
    DATA_RENTGEN__CONSUMER__GROUP_ID=data-rentgen
    DATA_RENTGEN__CONSUMER__FETCH_MAX_WAIT_MS=5000
    DATA_RENTGEN__CONSUMER__MAX_PARTITION_FETCH_BYTES=5MiB
    ```
    """

    topics_list: list[str] = Field(
        default=["input.runs"],
        description="List of Kafka topics to subscribe. Mutually exclusive with [topics_pattern][].",
    )
    topics_pattern: str | None = Field(
        default=None,
        description="Regex pattern of topics to subscribe. Mutually exclusive with [topics_list][].",
    )

    @model_validator(mode="after")
    def _check_topics(self):
        if not self.topics_list and not self.topics_pattern:
            msg = "input should contain either 'topics_list' or 'topics_pattern' field, both are empty"
            raise ValueError(msg)
        if self.topics_list and self.topics_pattern:
            msg = "input should contain either 'topics_list' or 'topics_pattern' field, both are set"
            raise ValueError(msg)
        return self

    group_id: str | None = Field(
        default="data-rentgen",
        description=textwrap.dedent(
            """
            Name of the consumer group to join for dynamic partition assignment (if enabled),
            and to use for fetching and committing offsets.
            If `None`, auto-partition assignment (via group coordinator) and offset commits are disabled.
            """,
        ),
    )

    # Defaults are copied from FastStream: https://github.com/airtai/faststream/blob/0.5.33/faststream/kafka/fastapi/fastapi.py#L618
    # But only options, related to consumer
    max_records: int | None = Field(
        default=None,
        description=textwrap.dedent(
            """
            Number of messages to consume as one batch.
            `None` means no limit applied.
            """,
        ),
    )
    fetch_max_bytes: ByteSize = Field(
        default=ByteSize(50 * 1024 * 1024),
        description="The maximum amount of data the server should return for a fetch request.",
    )
    fetch_min_bytes: ByteSize = Field(
        default=ByteSize(1),
        description=textwrap.dedent(
            """
            Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            [fetch_max_wait_ms][] for more data to accumulate.
            """,
        ),
    )
    fetch_max_wait_ms: int = Field(
        default=500,
        description=textwrap.dedent(
            """
            The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by [fetch_min_bytes][].
            """,
        ),
    )
    max_partition_fetch_bytes: ByteSize = Field(
        default=ByteSize(1024 * 1024),
        description=textwrap.dedent(
            """
            The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request `= #partitions * max_partition_fetch_bytes`.

            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition.
            """,
        ),
    )
    batch_timeout_ms: int = Field(
        default=200,
        description=textwrap.dedent(
            """
            Milliseconds spent waiting if data is not available in the buffer.
            If 0, returns immediately with any records that are available currently in the buffer,
            else returns empty.
            """,
        ),
    )
    auto_offset_reset: Literal["latest", "earliest", "none"] = Field(
        default="latest",
        description=textwrap.dedent(
            """
            A policy for resetting offsets on `OffsetOutOfRangeError` errors:

            * `earliest` will move to the oldest available message
            * `latest` will move to the most recent
            * `none` will raise an exception so you can handle this case
            """,
        ),
    )
    max_poll_interval_ms: int = Field(
        default=5 * 60 * 1000,
        description=textwrap.dedent(
            """
            Maximum allowed time between calls to consume messages in batches.
            If this interval is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member.
            If API methods block waiting for messages, that time
            does not count against this timeout.
            """,
        ),
    )
    session_timeout_ms: int = Field(
        default=10 * 1000,
        description=textwrap.dedent(
            """
            Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (`heartbeat.interval.ms`) to indicate its liveness to the broker.

            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance.

            The allowed range is configured with the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            """,
        ),
    )
    heartbeat_interval_ms: int = Field(
        default=3 * 1000,
        description=textwrap.dedent(
            """
            The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group.

            The value must be set lower than [session_timeout_ms][], but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances.
            """,
        ),
    )
    consumer_timeout_ms: int = Field(
        default=200,
        description=textwrap.dedent(
            """
            Maximum wait timeout for background fetching routine.
            Mostly defines how fast the system will see rebalance and
            request new data for new partitions.
            """,
        ),
    )
    isolation_level: Literal["read_uncommitted", "read_committed"] = Field(
        default="read_uncommitted",
        description=textwrap.dedent(
            """
            Controls how to read messages written
            transactionally.

            * `read_committed` - batch consumer will only return
              transactional messages which have been committed.

            * `read_uncommitted` (the default) - batch consumer will
              return all messages, even transactional messages which have been
              aborted.

            Non-transactional messages will be returned unconditionally in
            either mode.

            Messages will always be returned in offset order. Hence, in
            `read_committed` mode, batch consumer will only return
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, `read_committed` consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in `read_committed` the seek_to_end method will
            return the LSO.
            """,
        ),
    )
