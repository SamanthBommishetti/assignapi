from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, Boolean, DateTime
from app.core.database import Base
from datetime import datetime, timezone
import pytz

# Get the Asia/Kolkata timezone object
india_tz = pytz.timezone('Asia/Kolkata')
datetime_india = india_tz.localize(datetime.now())

class User(Base):
    __tablename__ = 'user'

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    email: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    password: Mapped[str] = mapped_column(String(128), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)  # SUPERADMIN, ADMIN, USER
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=datetime.now(india_tz), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime, default=datetime.now(india_tz), onupdate=datetime.now(india_tz))
    
    # Relationships
    analysis = relationship('Analysis', back_populates="user", cascade="all, delete-orphan")
    entity_user_map = relationship('EntityUserMap', back_populates='user')  # For entity scoping