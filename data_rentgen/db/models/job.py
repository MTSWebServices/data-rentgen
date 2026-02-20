# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from sqlalchemy import BigInteger, Column, Computed, ForeignKey, Index, String, Table, column, func, select
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, column_property, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.job_type import JobType
from data_rentgen.db.models.location import Location
from data_rentgen.db.models.tag_value import TagValue

JobTagValue: Table = Table(
    "job_tag_value",
    Base.metadata,
    Column("job_id", ForeignKey("job.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_value_id", ForeignKey("tag_value.id", ondelete="CASCADE"), primary_key=True),
    Index("ix__job_tag_value__tag_value_id", "tag_value_id"),
)


class Job(Base):
    __tablename__ = "job"
    __table_args__ = (
        Index("ix__job__location_id_name_lower", "location_id", func.lower(column("name")), unique=True),
        Index("ix__job__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        nullable=False,
        doc="Where job is located (Airflow instance, Spark cluster)",
    )
    location: Mapped[Location] = relationship(Location, lazy="noload")

    name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Job name, e.g. Airflow DAG name + task name, or Spark applicationName",
    )

    type_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("job_type.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Job type",
    )

    type = column_property(
        select(JobType.type).where(Column("type_id") == JobType.id).scalar_subquery(),
    )

    tag_values: Mapped[set[TagValue]] = relationship(
        secondary=JobTagValue,
        lazy="noload",
        doc="Job tag values",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            # Postgres treats values like `mydag.mytask` as a whole word (domain name),
            # which does not allow user to search by name parts like `mydag`.
            # Same for slashes which are treated like file paths.
            # Keep both original name and one without punctuation to allow both full match and partial match.
            #
            # Also 'english' dictionary performs stemming,
            # so name like 'my.dag.task' is converted to tsvector `'dag':2 'task':3`,
            # which does not match a tsquery like 'my:* & dag:* & task:*'.
            # Instead prefer 'simple' dictionary as it does not use stemming.
            "to_tsvector('simple'::regconfig, name || ' ' || (translate(name, '/.', '  ')))",
            persisted=True,
        ),
        nullable=False,
        deferred=True,
        doc="Full-text search vector",
    )
