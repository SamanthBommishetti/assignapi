from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, Integer, String, Boolean, DateTime
from app.core.database import Base
from datetime import datetime, timezone
import pytz

# Get the Asia/Kolkata timezone object
india_tz = pytz.timezone('Asia/Kolkata')
datetime_india = india_tz.localize(datetime.now())

class Dashboard(Base):
    __tablename__ = 'ai_dashboard'

    dashboard_id = mapped_column(Integer, primary_key=True, index=True)  # Changed to snake_case
    title = mapped_column(String(128), nullable=False)
    analysis_id = mapped_column(Integer, ForeignKey('ai_analysis.analysis_id'))  # Changed to snake_case
    created_at = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(india_tz), nullable=False)
    updated_at = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(india_tz), onupdate=lambda: datetime.now(india_tz))
    analysis = relationship('Analysis', back_populates="dashboard")  # Corrected back_populates
