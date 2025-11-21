from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, Integer, String, DateTime
from app.core.database import Base
from datetime import datetime
import pytz

# Get the Asia/Kolkata timezone object
india_tz = pytz.timezone('Asia/Kolkata')

class QueryResult(Base):
    __tablename__ = "ai_query_result"

    result_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    csv_file_name: Mapped[str] = mapped_column(String(512), nullable=False)
    chart_type: Mapped[str] = mapped_column(String(255), nullable=True)
    summary: Mapped[str] = mapped_column(String(1024), nullable=True)
    suggested_charts: Mapped[str] = mapped_column(String(255), nullable=True)  # Comma-separated str in DB
    query_id: Mapped[int] = mapped_column(Integer, ForeignKey("ai_query.query_id"), nullable=False)
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
    query = relationship("Query", back_populates="result")
