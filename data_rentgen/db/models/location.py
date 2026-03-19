# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Computed, Index, String
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base

if TYPE_CHECKING:
    from data_rentgen.db.models.address import Address


class Location(Base):
    """Some network location where data is bound to"""

    __tablename__ = "location"
    __table_args__ = (
        Index(None, "type", "name", unique=True),
        Index("ix__location__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        doc="Location type, e.g. kafka, postgres, hdfs",
    )
    name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Location name, e.g. cluster name",
    )
    external_id: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="External ID for integration with other systems",
    )

    addresses: Mapped[list["Address"]] = relationship(
        "Address",
        lazy="noload",
        back_populates="location",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            # Postgres threats host like `my.host.name` as a whole word,
            # which does not allow user to search by parts like `host.name`.
            # Keep both original name and one without punctuation to allow both full match and partial match.
            #
            # Also 'english' dictionary performs stemming,
            # so name like 'my.host.name' is converted to tsvector `'host':2 'name':3`,
            # which does not match a tsquery like 'my:* & host:* & name:*'.
            # Instead prefer 'simple' dictionary as it does not use stemming.
            """
            to_tsvector(
                'simple'::regconfig,
                type || ' ' ||
                name || ' ' || (translate(name, '/.', ' ')) || ' ' ||
                COALESCE(external_id, ''::text) || ' ' || (translate(COALESCE(external_id, ''::text), '/.', ' '))
            )
            """,
            persisted=True,
        ),
        nullable=False,
        deferred=True,
        doc="Full-text search vector",
    )
