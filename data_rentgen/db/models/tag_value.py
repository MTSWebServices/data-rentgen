# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import BigInteger, Computed, ForeignKey, Index, String, column, func
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.tag import Tag


class TagValue(Base):
    __tablename__ = "tag_value"
    __table_args__ = (
        Index("ix__tag_value__tag_id_value_lower", "tag_id", func.lower(column("value")), unique=True),
        Index("ix__tag_value__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tag_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("tag.id", ondelete="CASCADE"),
        nullable=False,
    )

    tag: Mapped[Tag] = relationship(
        Tag,
        lazy="noload",
        back_populates="tag_values",
        foreign_keys=[tag_id],
    )
    value: Mapped[str] = mapped_column(String(256), nullable=False)

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('simple'::regconfig, value || ' ' || translate(value, '/.', '  '))",
            persisted=True,
        ),
        nullable=False,
        deferred=True,
        doc="Full-text search vector for tag value",
    )
