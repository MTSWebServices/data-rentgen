# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Computed, Index, String, column, func
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base

if TYPE_CHECKING:
    from data_rentgen.db.models.tag_value import TagValue


class Tag(Base):
    __tablename__ = "tag"
    __table_args__ = (
        Index("ix__tag__name_lower", func.lower(column("name")), unique=True),
        Index("ix__tag__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(32), nullable=False)
    tag_values: Mapped[list["TagValue"]] = relationship(
        "TagValue",
        lazy="noload",
        back_populates="tag",
        order_by="TagValue.value",
    )
    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('simple'::regconfig, name || ' ' || translate(name, '/.', '  '))",
            persisted=True,
        ),
        nullable=False,
        deferred=True,
        doc="Full-text search vector for tag name",
    )
