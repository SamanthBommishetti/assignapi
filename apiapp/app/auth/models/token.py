# app/auth/models/token.py
# Added role to TokenData for completeness, though not strictly necessary since user is fetched.
from pydantic import BaseModel

class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    id: int | None = None
    email: str | None = None
    role: str | None = None
