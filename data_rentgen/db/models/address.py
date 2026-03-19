# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from sqlalchemy import BigInteger, Computed, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.location import Location


class Address(Base):
    __tablename__ = "address"
    __table_args__ = (
        UniqueConstraint("location_id", "url"),
        Index("ix__address__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Location the address is associated with",
    )

    location: Mapped[Location] = relationship(
        Location,
        lazy="noload",
        back_populates="addresses",
    )

    url: Mapped[str] = mapped_column(
        String,
        index=True,
        nullable=False,
        doc="Address in URL format",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            # Postgres threats host like `my.host.name` as a whole word,
            # which does not allow user to search by domain parts like `host.name`.
            # Keep both original name and one without punctuation to allow both full match and partial match.
            #
            # Also 'english' dictionary performs stemming,
            # so name like 'my.host.name' is converted to tsvector `'host':2 'name':3`,
            # which does not match a tsquery like 'my:* & host:* & name:*'.
            # Instead prefer 'simple' dictionary as it does not use stemming.
            "to_tsvector('simple'::regconfig, url || ' ' || (translate(url, '/.', '  ')))",
            persisted=True,
        ),
        nullable=False,
        deferred=True,
        doc="Full-text search vector",
    )
