from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class QueryBase(BaseModel):
    query_str: str
    analysis_id: int
    dashboard_id: int
    context_id: int

class QueryCreate(QueryBase):
    pass

class QuerySchema(QueryBase):
    query_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {  
        "from_attributes": True
    }

class QueryUpdate(BaseModel):
    query_str: Optional[str] = None
    analysis_id: Optional[int] = None
    dashboard_id: Optional[int] = None
    context_id: Optional[int] = None

    model_config = {
        "from_attributes": True
    }
