from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, ForeignKey, DateTime
from app.core.database import Base
from datetime import datetime
import pytz

# Get the Asia/Kolkata timezone object
india_tz = pytz.timezone('Asia/Kolkata')
datetime_india = india_tz.localize(datetime.now())

class Query(Base):
    __tablename__ = 'ai_query'

    query_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    query_str: Mapped[str] = mapped_column(String, nullable=False)
    analysis_id: Mapped[int] = mapped_column(Integer, ForeignKey("ai_analysis.analysis_id"), nullable=False)
    context_id: Mapped[int] = mapped_column(Integer, ForeignKey("com_context_table.context_id"), nullable=False)
    dashboard_id: Mapped[int] = mapped_column(Integer, ForeignKey("ai_dashboard.dashboard_id"), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=datetime.now(india_tz), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime, default=datetime.now(india_tz), onupdate=datetime.now(india_tz))

    result = relationship('QueryResult', back_populates="query", cascade="all, delete-orphan")
    context_table = relationship('ContextTable', back_populates="query")
