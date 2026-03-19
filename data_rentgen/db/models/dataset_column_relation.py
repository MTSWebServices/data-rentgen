# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Flag
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    SmallInteger,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class DatasetColumnRelationType(Flag):
    # See https://github.com/OpenLineage/OpenLineage/blob/1.27.0/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/TransformationInfo.java#L30-L40
    # Using IntFlag to avoid messing up with ARRAY type, bitwise OR is enough
    UNKNOWN = 1

    # Direct
    IDENTITY = 2
    TRANSFORMATION = 4
    TRANSFORMATION_MASKING = 8
    AGGREGATION = 16
    AGGREGATION_MASKING = 32

    # Indirect
    FILTER = 64
    JOIN = 128
    GROUP_BY = 256
    SORT = 512
    WINDOW = 1024
    CONDITIONAL = 2048


# no foreign keys to avoid scanning all the partitions
class DatasetColumnRelation(Base):
    __tablename__ = "dataset_column_relation"
    __table_args__ = (
        Index(
            None,
            Column("fingerprint"),
            Column("source_column"),
            # NULLs are distinct by default, we have to convert them to something else.
            # This is mostly for compatibility with PG <15, there is no `NULLS NOT DISTINCT` option
            func.coalesce(Column("target_column"), ""),
            unique=True,
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fingerprint: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=False,
        nullable=False,
        doc="Schema SHA-1 digest based used for grouping relations together. Currently this is in form of UUID",
    )

    source_column: Mapped[str] = mapped_column(
        String(length=255),
        index=False,
        nullable=False,
        doc="Source dataset column the data is originated from",
    )

    target_column: Mapped[str | None] = mapped_column(
        String(length=255),
        index=False,
        nullable=True,
        doc=(
            "Target dataset column the data is saved to. NULL means the entire target dataset depends on source column"
        ),
    )

    type: Mapped[DatasetColumnRelationType] = mapped_column(
        SmallInteger(),
        index=False,
        nullable=False,
        doc="Column transformation type",
    )
