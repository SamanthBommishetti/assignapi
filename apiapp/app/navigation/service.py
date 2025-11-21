# app/navigation/service.py
from sqlalchemy.orm import Session
from app.navigation.model import Navigation, NavigationQueryMap
from app.context_table.models.context_table import ContextNavigationMap
from app.navigation.schema import NavigationCreate, NavigationUpdate
from app.user.services.user_service import get_users_by_role_access
from app.general_query.model import GeneralQuery
from app.entity.model import Entity, EntityContextMap, EntityUserMap
from typing import List, Optional, Set

def _accessible_user_ids(db: Session, current_user) -> set[int]:
    users = get_users_by_role_access(db, current_user)
    return {u.user_id for u in users}

def _accessible_entity_ids(db: Session, current_user) -> Set[int]:
    """
    Return set of entity_id the current user belongs to.
    Uses: User → EntityUserMap
    """
    # get_users_by_role_access returns users visible to current_user (role + hierarchy)
    users = get_users_by_role_access(db, current_user)
    user_ids = {u.user_id for u in users}

    return {
        row[0] for row in db.query(EntityUserMap.entity_id)
        .filter(EntityUserMap.user_id.in_(user_ids))
        .all()
    }

def get_accessible_entity_ids(db: Session, current_user, selected_entity_id: Optional[int] = None) -> Set[int]:
    if current_user.role.upper() == "SUPERADMIN":
        if selected_entity_id == 0:
            return {e[0] for e in db.query(Entity).values(Entity.entity_id)}
        return {selected_entity_id}
    return _accessible_entity_ids(db, current_user)

# ----------------------------------------------------------------------
#  ROLE-SCOPED: Navigation → Query → Context → Entity → User
# ----------------------------------------------------------------------
def get_page_navigations_by_role_access(db: Session, current_user, page: int, size: int, selected_entity_id: Optional[int] = None):
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []

    offset = (page - 1) * size

    q = (
        db.query(Navigation)
        .join(NavigationQueryMap, Navigation.navigation_id == NavigationQueryMap.navigation_id)
        .join(GeneralQuery, NavigationQueryMap.query_id == GeneralQuery.query_id)
        .join(ContextNavigationMap, Navigation.navigation_id == ContextNavigationMap.navigation_id)
        .join(EntityContextMap, ContextNavigationMap.context_id == EntityContextMap.context_id)
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .distinct(Navigation.navigation_id)
    )
    if current_user.role.upper() != "SUPERADMIN":
        allowed = _accessible_user_ids(db, current_user)
        q = q.join(EntityUserMap, EntityContextMap.entity_id == EntityUserMap.entity_id) \
             .filter(EntityUserMap.user_id.in_(allowed))

    return q.offset(offset).limit(size).all()

def get_all_by_role_access(db: Session, current_user, selected_entity_id: Optional[int] = None):
    """Return **all** navigations the user can access (no pagination)."""
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []

    q = (
        db.query(Navigation)
        .join(NavigationQueryMap, Navigation.navigation_id == NavigationQueryMap.navigation_id)
        .join(GeneralQuery, NavigationQueryMap.query_id == GeneralQuery.query_id)
        .join(ContextNavigationMap, Navigation.navigation_id == ContextNavigationMap.navigation_id)
        .join(EntityContextMap, ContextNavigationMap.context_id == EntityContextMap.context_id)
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .distinct(Navigation.navigation_id)
    )
    if current_user.role.upper() != "SUPERADMIN":
        allowed = _accessible_user_ids(db, current_user)
        q = q.join(EntityUserMap, EntityContextMap.entity_id == EntityUserMap.entity_id) \
             .filter(EntityUserMap.user_id.in_(allowed))

    return q.all()

def get_navigation_count_by_role_access(db: Session, current_user, selected_entity_id: Optional[int] = None) -> int:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return 0

    q = (
        db.query(Navigation)
        .join(NavigationQueryMap, Navigation.navigation_id == NavigationQueryMap.navigation_id)
        .join(GeneralQuery, NavigationQueryMap.query_id == GeneralQuery.query_id)
        .join(ContextNavigationMap, Navigation.navigation_id == ContextNavigationMap.navigation_id)
        .join(EntityContextMap, ContextNavigationMap.context_id == EntityContextMap.context_id)
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .distinct(Navigation.navigation_id)
    )
    if current_user.role.upper() != "SUPERADMIN":
        allowed = _accessible_user_ids(db, current_user)
        q = q.join(EntityUserMap, EntityContextMap.entity_id == EntityUserMap.entity_id) \
             .filter(EntityUserMap.user_id.in_(allowed))

    return q.count()

def get_navigations_by_context_id_and_role(db: Session, context_id: int, current_user, selected_entity_id: Optional[int] = None):
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []

    q = (
        db.query(Navigation)
        .join(NavigationQueryMap, Navigation.navigation_id == NavigationQueryMap.navigation_id)
        .join(GeneralQuery, NavigationQueryMap.query_id == GeneralQuery.query_id)
        .join(ContextNavigationMap, Navigation.navigation_id == ContextNavigationMap.navigation_id)
        .join(EntityContextMap, ContextNavigationMap.context_id == EntityContextMap.context_id)
        .filter(ContextNavigationMap.context_id == context_id)
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .distinct(Navigation.navigation_id)
    )
    if current_user.role.upper() != "SUPERADMIN":
        allowed = _accessible_user_ids(db, current_user)
        q = q.join(EntityUserMap, EntityContextMap.entity_id == EntityUserMap.entity_id) \
             .filter(EntityUserMap.user_id.in_(allowed))

    return q.all()

def is_navigation_accessible(db: Session, current_user, nav: Navigation, selected_entity_id: Optional[int] = None) -> bool:
    if current_user.role.upper() == "SUPERADMIN":
        return True
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return False
    return (
        db.query(ContextNavigationMap)
        .join(EntityContextMap, ContextNavigationMap.context_id == EntityContextMap.context_id)
        .filter(
            ContextNavigationMap.navigation_id == nav.navigation_id,
            EntityContextMap.entity_id.in_(entity_ids)
        )
        .first() is not None
    )

# ----------------------------------------------------------------------
#  BASIC CRUD (unchanged)
# ----------------------------------------------------------------------
def get_all(db: Session):
    return db.query(Navigation).all()

def get_page_navigations(db: Session, page: int, size: int):
    offset = (page - 1) * size
    return db.query(Navigation).offset(offset).limit(size).all()

def get_navigation_count(db: Session) -> int:
    return db.query(Navigation).count()

def get_by_id(db: Session, navigation_id: int):
    return db.query(Navigation).filter(Navigation.navigation_id == navigation_id).first()

def get_by_stem(db: Session, navigation_stem: str):
    return db.query(Navigation).filter(Navigation.navigation_stem == navigation_stem).first()

def get_navigations_by_context_id(db: Session, context_id: int):
    return (
        db.query(Navigation)
        .join(ContextNavigationMap)
        .filter(ContextNavigationMap.context_id == context_id)
        .all()
    )

def create(db: Session, nav: NavigationCreate):
    db_nav = Navigation(**nav.dict())
    db.add(db_nav)
    db.commit()
    db.refresh(db_nav)
    return db_nav

def update(db: Session, navigation_id: int, nav: NavigationUpdate):
    db_nav = get_by_id(db, navigation_id)
    if not db_nav:
        return None
    update_data = nav.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_nav, key, value)
    db.commit()
    db.refresh(db_nav)
    return db_nav

def delete(db: Session, navigation_id: int) -> bool:
    db_nav = get_by_id(db, navigation_id)
    if not db_nav:
        return False
    db.delete(db_nav)
    db.commit()
    return True