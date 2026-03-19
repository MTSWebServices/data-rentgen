# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import (
    BigInteger,
    DateTime,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.dataset import Dataset
from data_rentgen.db.models.dataset_column_relation import DatasetColumnRelation
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.operation import Operation
from data_rentgen.db.models.run import Run


# no foreign keys to avoid scanning all the partitions
class ColumnLineage(Base):
    __tablename__ = "column_lineage"
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
        doc="Operation caused this column lineage",
    )
    operation: Mapped[Operation] = relationship(
        Operation,
        primaryjoin="ColumnLineage.operation_id == Operation.id",
        lazy="noload",
        foreign_keys=[operation_id],
    )

    run_id: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=False,
        doc="Run the operation is bound to",
    )
    run: Mapped[Run] = relationship(
        Run,
        primaryjoin="ColumnLineage.run_id == Run.id",
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
        primaryjoin="ColumnLineage.job_id == Job.id",
        lazy="noload",
        foreign_keys=[job_id],
    )

    source_dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Dataset the data is originated from",
    )
    source_dataset: Mapped[Dataset] = relationship(
        Dataset,
        primaryjoin="ColumnLineage.source_dataset_id == Dataset.id",
        lazy="noload",
        foreign_keys=[source_dataset_id],
    )

    target_dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Dataset the data is saved to",
    )
    target_dataset: Mapped[Dataset] = relationship(
        Dataset,
        primaryjoin="ColumnLineage.target_dataset_id == Dataset.id",
        lazy="noload",
        foreign_keys=[target_dataset_id],
    )

    fingerprint: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=False,
        nullable=False,
        doc="Dataset column relation fingerprint",
    )
    dataset_column_relations: Mapped[list[DatasetColumnRelation]] = relationship(
        DatasetColumnRelation,
        uselist=True,
        primaryjoin="ColumnLineage.fingerprint == DatasetColumnRelation.fingerprint",
        lazy="noload",
        foreign_keys=[fingerprint],
    )
