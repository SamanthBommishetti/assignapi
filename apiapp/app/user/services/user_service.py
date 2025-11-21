# app/user/services/user_service.py
from sqlalchemy.orm import Session
from app.auth.utils.auth_utils import get_password_hash
from app.user.models.user import User
from app.user.schemas.user import UserCreate
from app.entity.model import EntityUserMap
from typing import List
import logging

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
#  BASIC CRUD
# ----------------------------------------------------------------------
def get_all_users(db: Session) -> List[User]:
    return db.query(User).all()

def get_page_users(db: Session, page: int, size: int) -> List[User]:
    offset = (page - 1) * size
    return db.query(User).offset(offset).limit(size).all()

def get_user_by_id(db: Session, user_id: int) -> User | None:
    return db.query(User).filter(User.user_id == user_id).first()

def get_user_count(db: Session) -> int:
    return db.query(User).count()

def create_user(db: Session, user: UserCreate, creator_entity_id: int = None) -> User:
    """Create user, optionally assign to creator's entity."""
    db_user = User(
        name=user.name,
        email=user.email,
        role=user.role.upper(),
        is_active=user.is_active,
        password=get_password_hash(user.password)
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    if creator_entity_id:
        db.add(EntityUserMap(user_id=db_user.user_id, entity_id=creator_entity_id))
        db.commit()

    logger.debug(f"Created user {db_user.user_id}: {db_user.name} (role: {db_user.role})")
    return db_user

def delete_user(db: Session, user_id: int) -> int:
    db_user = get_user_by_id(db, user_id)
    if db_user:
        db.delete(db_user)
        db.commit()
        logger.debug(f"Deleted user {user_id}: {db_user.name}")
        return 0
    return -1

def get_user_by_email(db: Session, email: str) -> User | None:
    return db.query(User).filter(User.email == email).first()

# ----------------------------------------------------------------------
#  SCOPED ACCESS: USER → ENTITY (NO CROSS-ENTITY LEAK)
# ----------------------------------------------------------------------
def get_users_by_role_access(db: Session, current_user: User) -> List[User]:
    """
    - SUPERADMIN: All active users
    - ADMIN: Only active users in THEIR entity (exclude SUPERADMIN)
    - USER: Only themselves (if active)
    """
    if current_user.role.upper() == "SUPERADMIN":
        users = db.query(User).filter(User.is_active == True).all()
        logger.debug(f"SUPERADMIN {current_user.user_id} accessed all active users: count={len(users)}")
        return users

    if current_user.role.upper() == "ADMIN":
        # Get current admin's entities
        admin_entities = db.query(EntityUserMap.entity_id)\
            .filter(EntityUserMap.user_id == current_user.user_id)\
            .all()
        entity_ids = [e[0] for e in admin_entities]
        if not entity_ids:
            logger.warning(f"ADMIN {current_user.user_id} has no entities assigned")
            return []

        users = db.query(User).join(
            EntityUserMap, User.user_id == EntityUserMap.user_id
        ).filter(
            EntityUserMap.entity_id.in_(entity_ids),
            User.role != "SUPERADMIN",
            User.is_active == True
        ).distinct().all()
        logger.debug(f"ADMIN {current_user.user_id} accessed entity users: entities={entity_ids}, count={len(users)}")
        return users

    # USER: only self (if active)
    self_user = db.query(User).filter(User.user_id == current_user.user_id, User.is_active == True).first()
    logger.debug(f"USER {current_user.user_id} accessed self: {'active' if self_user else 'inactive'}")
    return [self_user] if self_user else []

def get_page_users_by_role_access(db: Session, current_user: User, page: int, size: int) -> List[User]:
    offset = (page - 1) * size

    if current_user.role.upper() == "SUPERADMIN":
        users = db.query(User).filter(User.is_active == True).offset(offset).limit(size).all()
        logger.debug(f"SUPERADMIN {current_user.user_id} paged all active users: page={page}, size={size}, count={len(users)}")
        return users

    if current_user.role.upper() == "ADMIN":
        admin_entities = db.query(EntityUserMap.entity_id)\
            .filter(EntityUserMap.user_id == current_user.user_id)\
            .all()
        entity_ids = [e[0] for e in admin_entities]
        if not entity_ids:
            return []

        users = db.query(User).join(
            EntityUserMap, User.user_id == EntityUserMap.user_id
        ).filter(
            EntityUserMap.entity_id.in_(entity_ids),
            User.role != "SUPERADMIN",
            User.is_active == True
        ).distinct().offset(offset).limit(size).all()
        logger.debug(f"ADMIN {current_user.user_id} paged entity users: page={page}, size={size}, count={len(users)}")
        return users

    # USER
    self_user = db.query(User).filter(User.user_id == current_user.user_id, User.is_active == True).offset(offset).limit(size).first()
    logger.debug(f"USER {current_user.user_id} paged self: {'found' if self_user else 'not found'}")
    return [self_user] if self_user else []

def get_user_count_by_role_access(db: Session, current_user: User) -> int:
    if current_user.role.upper() == "SUPERADMIN":
        count = db.query(User).filter(User.is_active == True).count()
        logger.debug(f"SUPERADMIN {current_user.user_id} counted all active users: {count}")
        return count

    if current_user.role.upper() == "ADMIN":
        admin_entities = db.query(EntityUserMap.entity_id)\
            .filter(EntityUserMap.user_id == current_user.user_id)\
            .all()
        entity_ids = [e[0] for e in admin_entities]
        if not entity_ids:
            return 0

        count = db.query(User).join(
            EntityUserMap, User.user_id == EntityUserMap.user_id
        ).filter(
            EntityUserMap.entity_id.in_(entity_ids),
            User.role != "SUPERADMIN",
            User.is_active == True
        ).distinct().count()
        logger.debug(f"ADMIN {current_user.user_id} counted entity users: {count}")
        return count

    # USER
    count = db.query(User).filter(User.user_id == current_user.user_id, User.is_active == True).count()
    logger.debug(f"USER {current_user.user_id} counted self: {count}")
    return count