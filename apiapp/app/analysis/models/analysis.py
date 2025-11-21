from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, Integer, String, DateTime
from app.core.database import Base
from datetime import datetime, timezone
import pytz

# Get the Asia/Kolkata timezone object
india_tz = pytz.timezone('Asia/Kolkata')
datetime_india = india_tz.localize(datetime.now())

class Analysis(Base):
    __tablename__ = 'ai_analysis'

    analysis_id = mapped_column(Integer, primary_key=True, index=True)
    title = mapped_column(String(128), nullable=False)
    user_id = mapped_column(Integer, ForeignKey('user.user_id'))
    created_at = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(india_tz), nullable=False)
    updated_at = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(india_tz), onupdate=lambda: datetime.now(india_tz))
    dashboard = relationship('Dashboard', back_populates="analysis", cascade="all, delete-orphan")
    user = relationship('User', back_populates="analysis")
