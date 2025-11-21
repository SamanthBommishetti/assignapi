# app/auth/services/auth_service.py
import logging
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from jwt import InvalidTokenError
from app.auth.utils.auth_utils import verify_password
from app.core.config_loader import settings
from datetime import datetime, timedelta, timezone
from app.core.database import get_db
from app.user.models.user import User
from app.user.services.user_service import get_user_by_email
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SECRET_KEY = settings.JWT_SECRET_KEY
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

def authenticate_user(db: Session, email: str, password: str):
    """Authenticate user with email and password."""
    user = get_user_by_email(db, email)
    if not user or not verify_password(password, user.password):
        logger.warning(f"Authentication failed for email: {email}")
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta):
    """Create JWT access token with expiration."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode.update({"exp": expire})
    if "role" in to_encode:
        to_encode["role"] = to_encode["role"].upper()
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """Get current user from JWT token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")  # Standard JWT claim for subject (email)
        if email is None:
            raise credentials_exception
    except (JWTError, InvalidTokenError):
        raise credentials_exception
    user = get_user_by_email(db, email)
    if user is None:
        raise credentials_exception
    return user

async def get_current_admin(current_user: User = Depends(get_current_user)):
    """Get current admin (SUPERADMIN or ADMIN)."""
    logger.info(f"Checking admin role for user: {current_user.email}, Role: {current_user.role}")
    if current_user.role.upper() not in ["SUPERADMIN", "ADMIN"]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return current_user