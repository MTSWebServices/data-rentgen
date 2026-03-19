# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import BigInteger, Column, Computed, ForeignKey, Index, String, Table, column, func
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.location import Location
from data_rentgen.db.models.tag_value import TagValue

DatasetTagValue: Table = Table(
    "dataset_tag_value",
    Base.metadata,
    Column("dataset_id", ForeignKey("dataset.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_value_id", ForeignKey("tag_value.id", ondelete="CASCADE"), primary_key=True),
    Index("ix__dataset_tag_value__tag_value_id", "tag_value_id"),
)


class Dataset(Base):
    __tablename__ = "dataset"
    __table_args__ = (
        Index("ix__dataset__location_id__name_lower", "location_id", func.lower(column("name")), unique=True),
        Index("ix__dataset__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        nullable=False,
        doc="Where dataset's data is actually located (database address, filesystem address)",
    )
    location: Mapped[Location] = relationship(
        Location,
        lazy="noload",
        foreign_keys=[location_id],
    )

    name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Dataset name, e.g. table name or filesystem path",
    )

    tag_values: Mapped[set[TagValue]] = relationship(
        secondary=DatasetTagValue,
        lazy="noload",
        doc="Dataset tag values",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            # Postgres treated values like `mydb.mytable` as a whole word (domain name),
            # which does not allow user to search by name parts like `mytable`.
            # Same for slashes which are treated like file paths.
            # Keep both original name and one without punctuation to allow both full match and partial match.
            #
            # Also 'english' dictionary performs stemming,
            # so name like 'my.database.table' is converted to tsvector `'databas':2 'tabl':3`,
            # which does not match a tsquery like 'my:* & database:* & table:*'.
            # Instead prefer 'simple' dictionary as it does not use stemming.
            "to_tsvector('simple'::regconfig, name || ' ' || (translate(name, '/.', '  ')))",
            persisted=True,
        ),
        nullable=False,
        deferred=True,
        doc="Full-text search vector",
    )
