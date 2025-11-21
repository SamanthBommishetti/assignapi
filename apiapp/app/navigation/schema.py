from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class NavigationBase(BaseModel):
    navigation_title: str = Field(..., max_length=128)
    navigation_stem: str = Field(..., max_length=128)
    navigation_description: Optional[str] = Field(None, max_length=256)

class NavigationCreate(NavigationBase):
    pass

class NavigationUpdate(BaseModel):
    navigation_title: Optional[str] = Field(None, max_length=128)
    navigation_stem: Optional[str] = Field(None, max_length=128)
    navigation_description: Optional[str] = Field(None, max_length=256)

class NavigationSchema(NavigationBase):
    navigation_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,
    }