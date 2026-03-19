# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from enum import Enum, IntEnum
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, Column, DateTime, Integer, PrimaryKeyConstraint, SmallInteger, String, select
from sqlalchemy.orm import Mapped, column_property, mapped_column, relationship
from sqlalchemy_utils import ChoiceType

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.run import Run
from data_rentgen.db.models.sql_query import SQLQuery


class OperationStatus(IntEnum):
    UNKNOWN = -1
    """No data about status"""

    STARTED = 0
    """Received START event"""

    SUCCEEDED = 1
    """Finished successfully"""

    FAILED = 2
    """Internal failure"""

    KILLED = 3
    """Killed externally, e.g. by user request or in case of OOM"""


class OperationType(str, Enum):
    BATCH = "BATCH"
    STREAMING = "STREAMING"

    def __str__(self) -> str:
        return str(self.value)


class Operation(Base):
    __tablename__ = "operation"
    __table_args__ = (
        # in most cases we filter rows by id, and sometimes by created_at
        PrimaryKeyConstraint("id", "created_at"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    id: Mapped[UUID] = mapped_column(SQL_UUID)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Timestamp component of UUID, used for table partitioning",
    )

    run_id: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=False,
        doc="Run operation is a part of",
    )
    run: Mapped[Run] = relationship(
        Run,
        primaryjoin="Operation.run_id == Run.id",
        lazy="noload",
        foreign_keys=[run_id],
    )

    status: Mapped[OperationStatus] = mapped_column(
        ChoiceType(OperationStatus, impl=SmallInteger()),
        nullable=False,
        default=OperationStatus.UNKNOWN,
        doc="Operation status",
    )

    name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="unknown",
        doc="Name of the operation, e.g. job name",
    )

    type: Mapped[OperationType] = mapped_column(
        ChoiceType(OperationType, impl=String(32)),
        nullable=False,
        default=OperationType.BATCH,
        doc="Type of the operation, e.g. BATCH, STREAMING",
    )

    position: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        doc="Sequentinal position of operation within the run, e.g. Spark jobId",
    )

    group: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Group of the operation, e.g. Spark jobGroup",
    )

    description: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Description of the operation, e.g. Spark jobDescription",
    )

    sql_query_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
        doc="Sql query of operation",
    )

    sql_query = column_property(
        select(SQLQuery.query).where(Column("sql_query_id") == SQLQuery.id).scalar_subquery(),
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Start time of the operation",
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="End time of the operation",
    )
