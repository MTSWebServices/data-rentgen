# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import BigInteger, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class JobType(Base):
    __tablename__ = "job_type"
    __table_args__ = (UniqueConstraint("type"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    type: Mapped[str] = mapped_column(
        String,
        index=True,
        nullable=False,
        doc="Job type, e.g. SPARK_APPLICATION, AIRFLOW_DAG",
    )
