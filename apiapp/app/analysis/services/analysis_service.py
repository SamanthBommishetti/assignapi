# app/analysis/services/analysis_service.py
from sqlalchemy.orm import Session
from typing import List, Set, Optional
from app.analysis.models.analysis import Analysis
from app.analysis.schemas.analysis import AnalysisCreate
from app.user.models.user import User
from app.user.services.user_service import get_users_by_role_access
from app.entity.model import Entity, EntityUserMap


# ----------------------------------------------------------------------
#  ACCESS HELPERS
# ----------------------------------------------------------------------
def _accessible_user_ids(db: Session, current_user: User) -> Set[int]:
    users = get_users_by_role_access(db, current_user)
    return {u.user_id for u in users}


def _accessible_entity_ids(db: Session, current_user: User) -> Set[int]:
    user_ids = _accessible_user_ids(db, current_user)
    return {
        row[0]
        for row in db.query(EntityUserMap.entity_id)
        .filter(EntityUserMap.user_id.in_(user_ids))
        .all()
    }


def get_accessible_entity_ids(
    db: Session, current_user: User, selected_entity_id: Optional[int] = None
) -> Set[int]:
    if current_user.role.upper() == "SUPERADMIN":
        if selected_entity_id == 0:
            return {e[0] for e in db.query(Entity).values(Entity.entity_id)}
        return {selected_entity_id}
    return _accessible_entity_ids(db, current_user)


# ----------------------------------------------------------------------
#  BASIC CRUD
# ----------------------------------------------------------------------
def get_analysis_by_id(db: Session, analysis_id: int) -> Analysis | None:
    return db.query(Analysis).filter(Analysis.analysis_id == analysis_id).first()


def get_page_analyses_by_user_id(
    db: Session,
    user_id: int,
    page: int,
    size: int,
    selected_entity_id: Optional[int] = None,
) -> List[Analysis]:
    offset = (page - 1) * size
    return (
        db.query(Analysis)
        .filter(Analysis.user_id == user_id)
        .offset(offset)
        .limit(size)
        .all()
    )


def get_all_analyses_by_user_id(
    db: Session, user_id: int, selected_entity_id: Optional[int] = None
) -> List[Analysis]:
    return db.query(Analysis).filter(Analysis.user_id == user_id).all()


def get_analysis_by_user_id_count(
    db: Session, user_id: int, selected_entity_id: Optional[int] = None
) -> int:
    return db.query(Analysis).filter(Analysis.user_id == user_id).count()


def create_analysis(
    db: Session,
    payload: AnalysisCreate,
    user_id: int,
    selected_entity_id: Optional[int] = None,
) -> Analysis:
    db_obj = Analysis(title=payload.title, user_id=user_id)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj


def delete_analysis(db: Session, analysis_id: int) -> int:
    obj = get_analysis_by_id(db, analysis_id)
    if obj:
        db.delete(obj)
        db.commit()
        return 0
    return -1


# ----------------------------------------------------------------------
#  ROLE + ENTITY SCOPED
# ----------------------------------------------------------------------
def get_analyses_by_role_access(
    db: Session, user: User, current_selection: Optional[int] = None
) -> List[Analysis]:
    entity_ids = get_accessible_entity_ids(db, user, current_selection)
    if not entity_ids:
        return []
    return (
        db.query(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .all()
    )


def get_page_analyses_by_role_access(
    db: Session, user: User, page: int, size: int, current_selection: Optional[int] = None
) -> List[Analysis]:
    entity_ids = get_accessible_entity_ids(db, user, current_selection)
    if not entity_ids:
        return []
    offset = (page - 1) * size
    return (
        db.query(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .offset(offset)
        .limit(size)
        .all()
    )


def get_analysis_count_by_role_access(
    db: Session, user: User, current_selection: Optional[int] = None
) -> int:
    entity_ids = get_accessible_entity_ids(db, user, current_selection)
    if not entity_ids:
        return 0
    return (
        db.query(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .count()
    )


def is_analysis_accessible(
    db: Session,
    current_user: User,
    analysis: Analysis,
    selected_entity_id: Optional[int] = None,
) -> bool:
    if current_user.role.upper() == "SUPERADMIN":
        return True
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    return (
        db.query(EntityUserMap)
        .filter(
            EntityUserMap.user_id == analysis.user_id,
            EntityUserMap.entity_id.in_(entity_ids),
        )
        .first()
        is not None
    )


def can_see_user_analyses(
    db: Session,
    current_user: User,
    target_user_id: int,
    selected_entity_id: Optional[int] = None,
) -> bool:
    if current_user.role.upper() == "SUPERADMIN":
        return True
    if current_user.role.upper() == "USER":
        return target_user_id == current_user.user_id

    # ADMIN → same entity
    admin_entities = {
        e.entity_id
        for e in db.query(EntityUserMap.entity_id)
        .filter(EntityUserMap.user_id == current_user.user_id)
        .all()
    }
    target_entities = {
        e.entity_id
        for e in db.query(EntityUserMap.entity_id)
        .filter(EntityUserMap.user_id == target_user_id)
        .all()
    }
    return bool(admin_entities & target_entities)