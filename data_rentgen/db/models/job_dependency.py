# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.job import Job


class JobDependency(Base):
    __tablename__ = "job_dependency"
    __table_args__ = (UniqueConstraint("from_job_id", "to_job_id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    from_job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("job.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="From job id",
    )
    from_job: Mapped[Job] = relationship(
        Job,
        lazy="noload",
        foreign_keys=[from_job_id],
    )

    to_job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("job.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="To job id",
    )
    to_job: Mapped[Job] = relationship(
        Job,
        lazy="noload",
        foreign_keys=[to_job_id],
    )

    type: Mapped[str] = mapped_column(
        String,
        nullable=True,
        doc="Dependency type",
    )
