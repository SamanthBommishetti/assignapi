# app/dashboard/services/dashboard_service.py
from sqlalchemy.orm import Session
from typing import List, Optional, Set
from app.dashboard.models.dashboard import Dashboard
from app.dashboard.schemas.dashboard import DashboardCreate, DashboardUpdate
from app.analysis.services import analysis_service
from app.analysis.models.analysis import Analysis
from app.user.services.user_service import get_users_by_role_access
from app.entity.model import Entity, EntityUserMap


def _accessible_user_ids(db: Session, current_user) -> Set[int]:
    users = get_users_by_role_access(db, current_user)
    return {u.user_id for u in users}


def _accessible_entity_ids(db: Session, current_user) -> Set[int]:
    user_ids = _accessible_user_ids(db, current_user)
    return {
        row[0]
        for row in db.query(EntityUserMap.entity_id)
        .filter(EntityUserMap.user_id.in_(user_ids))
        .all()
    }


def get_accessible_entity_ids(
    db: Session, current_user, selected_entity_id: Optional[int] = None
) -> Set[int]:
    if current_user.role.upper() == "SUPERADMIN":
        if selected_entity_id == 0:
            return {e[0] for e in db.query(Entity).values(Entity.entity_id)}
        return {selected_entity_id}
    return _accessible_entity_ids(db, current_user)


# ----------------------------------------------------------------------
#  BASIC CRUD
# ----------------------------------------------------------------------
def get_dashboard_by_id(db: Session, dashboard_id: int) -> Dashboard | None:
    return db.query(Dashboard).filter(Dashboard.dashboard_id == dashboard_id).first()


def get_page_dashboards_by_analysis_id(
    db: Session,
    analysis_id: int,
    page: int,
    size: int,
    selected_entity_id: Optional[int] = None,
) -> List[Dashboard]:
    offset = (page - 1) * size
    return (
        db.query(Dashboard)
        .filter(Dashboard.analysis_id == analysis_id)
        .offset(offset)
        .limit(size)
        .all()
    )


def get_all_dashboards_by_analysis_id(
    db: Session, analysis_id: int, selected_entity_id: Optional[int] = None
) -> List[Dashboard]:
    return db.query(Dashboard).filter(Dashboard.analysis_id == analysis_id).all()


def get_dashboard_count_by_analysis_id(
    db: Session, analysis_id: int, selected_entity_id: Optional[int] = None
) -> int:
    return db.query(Dashboard).filter(Dashboard.analysis_id == analysis_id).count()


def create_dashboard(
    db: Session, payload: DashboardCreate, selected_entity_id: Optional[int] = None
) -> Dashboard:
    obj = Dashboard(title=payload.title, analysis_id=payload.analysis_id)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def delete_dashboard(db: Session, dashboard_id: int) -> int:
    obj = get_dashboard_by_id(db, dashboard_id)
    if obj:
        db.delete(obj)
        db.commit()
        return 0
    return -1


def update_dashboard(
    db: Session,
    dashboard_id: int,
    payload: DashboardUpdate,
    selected_entity_id: Optional[int] = None,
) -> Dashboard:
    obj = get_dashboard_by_id(db, dashboard_id)
    if not obj:
        return None
    data = payload.dict(exclude_unset=True)
    for k, v in data.items():
        setattr(obj, k, v)
    db.commit()
    db.refresh(obj)
    return obj


# ----------------------------------------------------------------------
#  ROLE + ENTITY SCOPED
# ----------------------------------------------------------------------
def get_dashboards_by_role_access(
    db: Session, current_user, selected_entity_id: Optional[int] = None
) -> List[Dashboard]:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []
    return (
        db.query(Dashboard)
        .join(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .all()
    )


def get_page_dashboards_by_role_access(
    db: Session, current_user, page: int, size: int, selected_entity_id: Optional[int] = None
) -> List[Dashboard]:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []
    offset = (page - 1) * size
    return (
        db.query(Dashboard)
        .join(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .offset(offset)
        .limit(size)
        .all()
    )


def get_dashboard_count_by_role_access(
    db: Session, current_user, selected_entity_id: Optional[int] = None
) -> int:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return 0
    return (
        db.query(Dashboard)
        .join(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .count()
    )


def is_dashboard_accessible(
    db: Session,
    current_user,
    dashboard: Dashboard,
    selected_entity_id: Optional[int] = None,
) -> bool:
    if current_user.role.upper() == "SUPERADMIN":
        return True
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    analysis = analysis_service.get_analysis_by_id(db, dashboard.analysis_id)
    return (
        db.query(EntityUserMap)
        .filter(
            EntityUserMap.user_id == analysis.user_id,
            EntityUserMap.entity_id.in_(entity_ids),
        )
        .first()
        is not None
    )