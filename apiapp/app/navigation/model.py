# app/navigation/model.py
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, ForeignKey
from app.core.database import Base
from datetime import datetime, timezone
import pytz
from sqlalchemy import DateTime
from sqlalchemy.orm import relationship
from sqlalchemy import Integer, ForeignKey
from app.general_query.model import GeneralQuery

india_tz = pytz.timezone('Asia/Kolkata')
datetime_india = india_tz.localize(datetime.now())

class Navigation(Base):
    __tablename__ = 'gen_navigation'

    navigation_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    navigation_title: Mapped[str] = mapped_column(String(128), nullable=False)
    navigation_stem: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    navigation_description: Mapped[str] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, onupdate=datetime_india)
    navigation_query_map = relationship('NavigationQueryMap', back_populates='navigation', cascade='all, delete-orphan')
    context_navigation_map = relationship('ContextNavigationMap', back_populates='navigation', cascade='all, delete-orphan')
    general_queries: Mapped[list["GeneralQuery"]] = relationship("GeneralQuery", secondary="gen_navigation_query_map", back_populates="navigation")

class NavigationQueryMap(Base):
    __tablename__ = 'gen_navigation_query_map'

    navigation_query_map_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    navigation_id: Mapped[int] = mapped_column(ForeignKey('gen_navigation.navigation_id'), nullable=False)
    query_id: Mapped[int] = mapped_column(ForeignKey('gen_query.query_id'), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime_india, onupdate=datetime_india)
    navigation = relationship('Navigation', back_populates='navigation_query_map')
    general_query = relationship("GeneralQuery", back_populates="navigation_query_map")
