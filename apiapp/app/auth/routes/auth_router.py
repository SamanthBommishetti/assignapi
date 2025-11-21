from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import timedelta

from app.core.database import get_db
from app.auth.services.auth_service import authenticate_user, create_access_token
from app.user.models.user import User
from app.auth.services.auth_service import get_current_user  # For /me endpoint
from app.core.config_loader import settings  # For ACCESS_TOKEN_EXPIRE_MINUTES

auth_router = APIRouter(
    prefix="/auth",
    tags=["auth"]
)

@auth_router.post("/login", response_model=dict)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """Login and return JWT token."""
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.email, "role": user.role},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@auth_router.get("/me", response_model=dict)
async def read_users_me(current_user: User = Depends(get_current_user)):
    """Get current user info."""
    return {
        "user_id": current_user.user_id,
        "name": current_user.name,
        "email": current_user.email,
        "role": current_user.role,
        "is_active": current_user.is_active
    }