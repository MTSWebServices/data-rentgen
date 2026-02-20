# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from enum import Enum, IntEnum
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import (
    BigInteger,
    Computed,
    DateTime,
    Index,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    select,
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, aliased, foreign, mapped_column, relationship
from sqlalchemy_utils import ChoiceType

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.user import User


class RunStatus(IntEnum):
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


class RunStartReason(str, Enum):
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"

    def __str__(self) -> str:
        return str(self.value)


# no foreign keys to avoid scanning all the partitions
class Run(Base):
    __tablename__ = "run"
    __table_args__ = (
        # in most cases we filter rows by id, and sometimes by created_at
        PrimaryKeyConstraint("id", "created_at"),
        Index("ix__run__search_vector", "search_vector", postgresql_using="gin"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    id: Mapped[UUID] = mapped_column(SQL_UUID)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Timestamp component of UUID, used for table partitioning",
    )

    job_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Job the run is associated with",
    )
    job: Mapped[Job] = relationship(
        Job,
        primaryjoin="Run.job_id == Job.id",
        lazy="noload",
        foreign_keys=[job_id],
    )

    parent_run_id: Mapped[UUID | None] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=True,
        doc="Parent of current run, e.g. Airflow task run which started Spark application",
    )
    parent_run: Mapped["Run | None"] = relationship(
        "Run",
        primaryjoin="Run.parent_run_id == Run.id",
        lazy="noload",
        foreign_keys=[parent_run_id],
    )

    status: Mapped[RunStatus] = mapped_column(
        ChoiceType(RunStatus, impl=SmallInteger()),
        nullable=False,
        default=RunStatus.UNKNOWN,
        doc="Run status info",
    )

    external_id: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="External ID of the run, e.g. Spark applicationId",
    )
    attempt: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Attempt number of the run",
    )
    persistent_log_url: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Persistent log url of the run, like Spark history server url, optional",
    )
    running_log_url: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Log url of the run in progress, like Spark session UI url, optional",
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Start time of the run",
    )
    started_by_user_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="User who started the run",
    )
    started_by_user: Mapped[User | None] = relationship(
        "User",
        primaryjoin="Run.started_by_user_id == User.id",
        lazy="noload",
        foreign_keys=[started_by_user_id],
    )
    start_reason: Mapped[RunStartReason | None] = mapped_column(
        ChoiceType(RunStartReason, impl=String(32)),
        nullable=True,
        doc="Start reason of the run, e.g. MANUAL or AUTOMATIC",
    )

    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="End time of the run",
    )
    end_reason: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="End reason of the run, e.g. exception string",
    )
    expected_start_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp representing the nominal start time (included) of the run. AKA the schedule time",
    )
    expected_end_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc=(
            "Timestamp representing the nominal end time (excluded) of the run."
            "Represents the scheduled/expected time, not the actual end time."
        ),
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            # Postgres treats values like `my.app.id` as a whole word (domain name),
            # which does not allow user to search by parts like `some` or `value`.
            # Same for slashes which are treated like file paths.
            # Keep both original name and one without punctuation to allow both full match and partial match.
            #
            # Also 'english' dictionary performs stemming,
            # so name like 'my.app.id' is converted to tsvector `'app':2 'id':3`,
            # which does not match a tsquery like 'my:* & app:* & id:*'.
            # Instead prefer 'simple' dictionary as it does not use stemming.
            "to_tsvector('simple'::regconfig, external_id || ' ' || (translate(external_id, '/.', '  ')))",
            persisted=True,
        ),
        nullable=True,
        deferred=True,
        doc="Full-text search vector",
    )


# DISTINCT ON is much more efficiend than (run_id == last(run_id) PARTITION BY job_id)
last_run_query = (
    select(Run).distinct(Run.job_id).order_by(Run.job_id.asc(), Run.id.desc(), Run.created_at.desc()).alias("last_run")
)
JobLastRun = aliased(Run, last_run_query, flat=True)
Job.last_run = relationship(
    JobLastRun,
    primaryjoin=Job.id == foreign(JobLastRun.job_id),
    lazy="noload",
    viewonly=True,
    uselist=False,
)
