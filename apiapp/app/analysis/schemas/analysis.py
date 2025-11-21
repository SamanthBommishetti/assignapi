from pydantic import BaseModel
from datetime import datetime

class AnalysisBase(BaseModel):
    title: str
    user_id: int  # Changed to snake_case for consistency with model

class AnalysisCreate(AnalysisBase):
    pass

class AnalysisSchema(AnalysisBase):
    analysis_id: int  # Changed to snake_case
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True