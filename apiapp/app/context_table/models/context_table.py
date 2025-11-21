from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, Integer, String, DateTime
from app.core.database import Base
from datetime import datetime
import pytz

india_tz = pytz.timezone('Asia/Kolkata')

class ContextTable(Base):
    __tablename__ = 'com_context_table'

    context_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    schema_info: Mapped[str] = mapped_column(String, nullable=False)
    llm_schema: Mapped[str] = mapped_column(String, nullable=False)

    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(india_tz),
        nullable=False,
    )
    updated_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(india_tz),
        onupdate=lambda: datetime.now(india_tz),
        nullable=False
    )

    # Internal relationships – NOT exposed via API
    query = relationship('Query', back_populates="context_table")
    entity_map = relationship('EntityContextMap', back_populates="context")
    context_navigation_map = relationship('ContextNavigationMap', back_populates="context_table")

class ContextNavigationMap(Base):
    __tablename__ = 'gen_context_navigation_map'

    context_navigation_map_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    context_id: Mapped[int] = mapped_column(ForeignKey('com_context_table.context_id'), nullable=False)
    navigation_id: Mapped[int] = mapped_column(ForeignKey('gen_navigation.navigation_id'), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(india_tz),
        nullable=False
    )
    updated_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(india_tz),
        onupdate=lambda: datetime.now(india_tz),
        nullable=False
    )
    # Relationship back to ContextTable
    context_table = relationship('ContextTable', back_populates="context_navigation_map")
    navigation = relationship('Navigation', back_populates="context_navigation_map")