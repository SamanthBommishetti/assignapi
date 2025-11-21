from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class ContextTableBase(BaseModel):
    name: str = Field(..., max_length=128)
    schema_info: str
    llm_schema: str

class ContextTableCreate(ContextTableBase):
    pass

class ContextTableUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=128)
    schema_info: Optional[str] = None
    llm_schema: Optional[str] = None

class ContextTableSchema(ContextTableBase):
    context_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,
    }

# Keep ContextNavigationMap schemas for internal use (optional)
class ContextNavigationMapBase(BaseModel):
    context_id: int
    navigation_id: int

class ContextNavigationMapCreate(ContextNavigationMapBase):
    pass

class ContextNavigationMapUpdate(ContextNavigationMapBase):
    context_id: Optional[int] = None
    navigation_id: Optional[int] = None

class ContextNavigationMapSchema(ContextNavigationMapBase):
    context_navigation_map_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,
    }