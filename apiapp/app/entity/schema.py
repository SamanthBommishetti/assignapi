from pydantic import BaseModel, Field
from datetime import datetime

# Base models for common fields
class EntityBase(BaseModel):
    name: str = Field(..., max_length=128)
    description: str | None = Field(None, max_length=256)

class EntityContextMapBase(BaseModel):
    entity_id: int
    context_id: int

class EntityNavigationMapBase(BaseModel):
    entity_id: int
    navigation_id: int

class EntityUserMapBase(BaseModel):
    entity_id: int
    user_id: int

# Create models
class EntityCreate(EntityBase):
    pass

class EntityContextMapCreate(EntityContextMapBase):
    pass

class EntityNavigationMapCreate(EntityNavigationMapBase):
    pass

class EntityUserMapCreate(EntityUserMapBase):
    pass

# Update models
class EntityUpdate(EntityBase):
    name: str | None = Field(None, max_length=128)
    description: str | None = Field(None, max_length=256)

class EntityContextMapUpdate(EntityContextMapBase):
    entity_id: int | None = None
    context_id: int | None = None

class EntityNavigationMapUpdate(EntityNavigationMapBase):
    entity_id: int | None = None
    navigation_id: int | None = None

class EntityUserMapUpdate(EntityUserMapBase):
    entity_id: int | None = None
    user_id: int | None = None

# Response models
class EntitySchema(EntityBase):
    entity_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class EntityContextMapSchema(EntityContextMapBase):
    entity_context_map_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class EntityNavigationMapSchema(EntityNavigationMapBase):
    entity_navigation_map_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class EntityUserMapSchema(EntityUserMapBase):
    entity_user_map_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True