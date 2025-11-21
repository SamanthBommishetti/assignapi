# app/context_table/services/context_table_service.py
from sqlalchemy.orm import Session, joinedload
from app.context_table.models.context_table import ContextTable, ContextNavigationMap
from app.context_table.schemas.context_table import (
    ContextTableCreate, ContextTableUpdate, ContextTableSchema
)
from app.entity.model import Entity, EntityContextMap, EntityUserMap
from app.user.services.user_service import get_users_by_role_access
from typing import List, Set, Optional

# ----------------------------------------------------------------------
#  ACCESS HELPERS
# ----------------------------------------------------------------------
def _accessible_entity_ids(db: Session, current_user, selected_entity_id: Optional[int] = None) -> Set[int]:
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
#  SUPERADMIN-ONLY READS (no filtering)
# ----------------------------------------------------------------------
def get_all_context_tables(db: Session) -> List[ContextTable]:
    return db.query(ContextTable).options(joinedload(ContextTable.context_navigation_map)).all()

def get_page_context_tables(db: Session, page: int, size: int) -> List[ContextTable]:
    offset = (page - 1) * size
    return (
        db.query(ContextTable)
        .offset(offset)
        .limit(size)
        .options(joinedload(ContextTable.context_navigation_map))
        .all()
    )

def get_context_tables_count(db: Session) -> int:
    return db.query(ContextTable).count()

# ----------------------------------------------------------------------
#  ROLE-SCOPED READS (User → Entity → ContextTable)
# ----------------------------------------------------------------------
def get_all_context_tables_by_role(db: Session, current_user, selected_entity_id: Optional[int] = None) -> List[ContextTable]:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []

    return (
        db.query(ContextTable)
        .join(EntityContextMap, ContextTable.context_id == EntityContextMap.context_id)
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .options(joinedload(ContextTable.context_navigation_map))
        .all()
    )


def get_page_context_tables_by_role(db: Session, current_user, page: int, size: int, selected_entity_id: Optional[int] = None) -> List[ContextTable]:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []

    offset = (page - 1) * size
    return (
        db.query(ContextTable)
        .join(EntityContextMap, ContextTable.context_id == EntityContextMap.context_id)
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .offset(offset)
        .limit(size)
        .options(joinedload(ContextTable.context_navigation_map))
        .all()
    )

def get_context_tables_count_by_role(db: Session, current_user, selected_entity_id: Optional[int] = None) -> int:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return 0

    return (
        db.query(ContextTable.context_id)
        .join(EntityContextMap, ContextTable.context_id == EntityContextMap.context_id)
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .count()
    )

# ----------------------------------------------------------------------
#  SINGLE GET + ACCESS CHECK
# ----------------------------------------------------------------------
def get_context_table_by_id(db: Session, context_id: int) -> ContextTable | None:
    return db.query(ContextTable).filter(ContextTable.context_id == context_id).first()

def is_context_accessible(db: Session, current_user, ctx: ContextTable, selected_entity_id: Optional[int] = None) -> bool:
    if current_user.role.upper() == "SUPERADMIN":
        return True

    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return False

    return db.query(EntityContextMap).filter(
        EntityContextMap.context_id == ctx.context_id,
        EntityContextMap.entity_id.in_(entity_ids)
    ).first() is not None

# ----------------------------------------------------------------------
#  CREATE / UPDATE / DELETE
# ----------------------------------------------------------------------
def get_context_table_by_name(db: Session, name: str) -> ContextTable | None:
    return db.query(ContextTable).filter(ContextTable.name.ilike(f"%{name}%")).first()

def create_context_table(db: Session, ctx: ContextTableCreate) -> ContextTable:
    db_ctx = ContextTable(**ctx.dict())
    db.add(db_ctx)
    db.commit()
    db.refresh(db_ctx)
    return db_ctx

def update_context_table(db: Session, context_id: int, ctx: ContextTableUpdate) -> ContextTable | None:
    db_ctx = get_context_table_by_id(db, context_id)
    if not db_ctx:
        return None
    data = ctx.dict(exclude_unset=True)
    for k, v in data.items():
        setattr(db_ctx, k, v)
    db.commit()
    db.refresh(db_ctx)
    return db_ctx

def delete_context_table(db: Session, context_id: int) -> bool:
    db_ctx = get_context_table_by_id(db, context_id)
    if not db_ctx:
        return False
    db.delete(db_ctx)
    db.commit()
    return True