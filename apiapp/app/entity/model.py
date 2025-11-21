# app/entity/models.py
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, ForeignKey, DateTime
from app.core.database import Base
from datetime import datetime, timezone
import pytz

india_tz = pytz.timezone('Asia/Kolkata')
datetime_india = india_tz.localize(datetime.now())

class Entity(Base):
    __tablename__ = 'com_entity'

    entity_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, onupdate=datetime_india)
    entity_context_map = relationship('EntityContextMap', back_populates='entity', cascade='all, delete-orphan')
    entity_user_map = relationship('EntityUserMap', back_populates='entity', cascade='all, delete-orphan')

class EntityContextMap(Base):
    __tablename__ = 'com_entity_context_map'

    entity_context_map_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    entity_id: Mapped[int] = mapped_column(ForeignKey('com_entity.entity_id'), nullable=False)
    context_id: Mapped[int] = mapped_column(ForeignKey('com_context_table.context_id'), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, onupdate=datetime_india)
    entity = relationship('Entity', back_populates='entity_context_map')
    context = relationship('ContextTable', back_populates='entity_map')

class EntityUserMap(Base):
    __tablename__ = 'com_entity_user_map'

    entity_user_map_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    entity_id: Mapped[int] = mapped_column(ForeignKey('com_entity.entity_id'), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey('user.user_id'), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, onupdate=datetime_india)
    entity = relationship('Entity', back_populates='entity_user_map')
    user = relationship('User', back_populates='entity_user_map')  # Bidirectional