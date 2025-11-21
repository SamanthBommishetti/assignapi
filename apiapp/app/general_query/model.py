# app/general_query/model.py
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime
from app.core.database import Base
from datetime import datetime
import pytz
from typing import Optional

india_tz = pytz.timezone('Asia/Kolkata')

class GeneralQuery(Base):
    __tablename__ = 'gen_query'

    query_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    query_sql_string: Mapped[str] = mapped_column(String(1024), nullable=False)
    query_str: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    csv_file_path: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    chart_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    suggested_chart_types: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: india_tz.localize(datetime.now()), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: india_tz.localize(datetime.now()), onupdate=lambda: india_tz.localize(datetime.now()))

    # Use string references for relationships
    navigation: Mapped[list["Navigation"]] = relationship(
        "Navigation", secondary="gen_navigation_query_map", back_populates="general_queries"
    )
    navigation_query_map: Mapped[list["NavigationQueryMap"]] = relationship(
        "NavigationQueryMap", back_populates="general_query", cascade="all, delete-orphan"
    )
