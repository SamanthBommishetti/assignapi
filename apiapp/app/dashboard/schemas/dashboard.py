# app/dashboard/schemas/dashboard.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class DashboardBase(BaseModel):
    title: str
    analysis_id: int

class DashboardCreate(DashboardBase):
    pass

class DashboardSchema(DashboardBase):
    dashboard_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class DashboardUpdate(BaseModel):
    title: Optional[str] = None
    analysis_id: Optional[int] = None 
