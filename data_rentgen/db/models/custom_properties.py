# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import BigInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class CustomProperties(Base):
    __tablename__ = "custom_properties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
