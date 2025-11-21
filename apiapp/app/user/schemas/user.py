from pydantic import BaseModel, EmailStr
from datetime import datetime

class UserBase(BaseModel):
    name: str
    email: EmailStr
    role: str  = "USER"
    is_active: bool

class UserCreate(UserBase):
    password: str

class UserSchema(UserBase):
    user_id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
