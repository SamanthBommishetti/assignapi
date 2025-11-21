# app/user/routes/user_router.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi import Query
from sqlalchemy.orm import Session
from typing import List
from app.auth.services.auth_service import get_current_user, get_current_admin
from app.core.database import get_db
from app.user.models.user import User
from app.user.schemas.user import UserSchema, UserCreate
from app.user.services import user_service
import logging

logger = logging.getLogger(__name__)

user_router = APIRouter(
    prefix='/user',
    tags=['Users']
)

@user_router.get("/me", response_model=UserSchema, summary="Get Current User")
async def current_user_details(current_user: User = Depends(get_current_user)):
    return current_user

@user_router.get("/list", response_model=List[UserSchema], summary="Get Paged User List (Role-Scoped)")
def get_users_list(current_user: User = Depends(get_current_user), db: Session = Depends(get_db), page: int = Query(1, ge=1), size: int = Query(10, ge=1)):
    users = user_service.get_page_users_by_role_access(db, current_user, page, size)
    if not users:
        raise HTTPException(status_code=404, detail="No accessible users found")
    logger.info(f"User {current_user.user_id} ({current_user.role}) fetched paged users: page={page}, size={size}, count={len(users)}")
    return users

@user_router.get("/all", response_model=List[UserSchema], summary="Get All User List (Role-Scoped)")
def get_users_list_all(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    users = user_service.get_users_by_role_access(db, current_user)
    if not users:
        raise HTTPException(status_code=404, detail="No accessible users found")
    logger.info(f"User {current_user.user_id} ({current_user.role}) fetched all accessible users: count={len(users)}")
    return users

@user_router.get("/count", summary="Get User Count (Role-Scoped)")
def get_user_count(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> int:
    count = user_service.get_user_count_by_role_access(db, current_user)
    if count == 0:
        raise HTTPException(status_code=404, detail="No accessible users found")
    logger.info(f"User {current_user.user_id} ({current_user.role}) fetched user count: {count}")
    return count

@user_router.get("/{user_id}", response_model=UserSchema, summary="Get User by User ID (Scoped)")
def get_user_by_id(user_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    target_user = user_service.get_user_by_id(db, user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail=f"User with user_id: {user_id} not found")
    
    # Scoped check
    accessible_users = user_service.get_users_by_role_access(db, current_user)
    if target_user not in accessible_users:
        raise HTTPException(status_code=403, detail="User not accessible")
    
    logger.info(f"User {current_user.user_id} ({current_user.role}) fetched user {user_id}: {target_user.name}")
    return target_user

@user_router.delete("/{user_id}", summary="Delete User by User ID (Admin-Scoped)")
def delete_user(user_id: int, current_user: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    target_user = user_service.get_user_by_id(db, user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail=f"User with user_id: {user_id} not found")
    
    # Ensure target is in accessible list for admin
    accessible_users = user_service.get_users_by_role_access(db, current_user)
    if target_user not in accessible_users:
        raise HTTPException(status_code=403, detail="Cannot delete user outside scope")
    
    # Prevent self-deletion for super admins (optional safeguard)
    if current_user.role.upper() == "SUPERADMIN" and user_id == current_user.user_id:
        raise HTTPException(status_code=400, detail="Super admins cannot delete themselves")
    
    status = user_service.delete_user(db, user_id)
    if status == 0:
        logger.info(f"User {current_user.user_id} ({current_user.role}) deleted user {user_id}: {target_user.name}")
        return {"message": "User deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail=f"User with user_id: {user_id} could not be deleted")

@user_router.post("/create", response_model=UserSchema, summary="Create User (Admin-Scoped)")
def user_create(user: UserCreate, current_user: User = Depends(get_current_admin), db: Session = Depends(get_db)):
    existing_user = user_service.get_user_by_email(db, user.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Assign to creator's first entity
    creator_entity_id = None
    if current_user.entity_user_map:
        creator_entity_id = current_user.entity_user_map[0].entity_id
    
    if not creator_entity_id and current_user.role.upper() != "SUPERADMIN":
        raise HTTPException(status_code=400, detail="Admin must be mapped to an entity")
    
    new_user = user_service.create_user(db, user, creator_entity_id)
    if new_user is None:
        raise HTTPException(status_code=500, detail="User could not be created")
    
    logger.info(f"User {current_user.user_id} ({current_user.role}) created user {new_user.user_id}: {new_user.name}")
    return new_user