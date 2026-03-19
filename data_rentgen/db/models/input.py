# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, DateTime, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.dataset import Dataset
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.operation import Operation
from data_rentgen.db.models.run import Run
from data_rentgen.db.models.schema import Schema


# no foreign keys to avoid scanning all the partitions
class Input(Base):
    __tablename__ = "input"
    __table_args__ = (
        # in most cases we filter rows by created_at, and never by id
        PrimaryKeyConstraint("created_at", "id"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Timestamp component of UUID, used for table partitioning",
    )
    id: Mapped[UUID] = mapped_column(SQL_UUID)

    operation_id: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=False,
        doc="Operation caused this input",
    )
    operation: Mapped[Operation] = relationship(
        Operation,
        primaryjoin="Input.operation_id == Operation.id",
        lazy="noload",
        foreign_keys=[operation_id],
    )

    run_id: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=False,
        doc="Run caused this input operation",
    )
    run: Mapped[Run] = relationship(
        Run,
        primaryjoin="Input.run_id == Run.id",
        lazy="noload",
        foreign_keys=[run_id],
    )

    job_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Parent job of run",
    )
    job: Mapped[Job] = relationship(
        Job,
        primaryjoin="Input.job_id == Job.id",
        lazy="noload",
        foreign_keys=[job_id],
    )

    dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Dataset the input is performed against",
    )
    dataset: Mapped[Dataset] = relationship(
        Dataset,
        primaryjoin="Input.dataset_id == Dataset.id",
        lazy="noload",
        foreign_keys=[dataset_id],
    )

    schema_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
        doc="Schema the input is performed with, if any",
    )
    schema: Mapped[Schema | None] = relationship(
        Schema,
        primaryjoin="Input.schema_id == Schema.id",
        lazy="noload",
        foreign_keys=[schema_id],
    )

    num_bytes: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during input, in bytes",
    )

    num_rows: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during input, in rows",
    )

    num_files: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during input, in rows",
    )
