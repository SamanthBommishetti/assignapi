# app/dashboard/router.py
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from typing import List
from app.auth.services.auth_service import get_current_user
from app.core.database import get_db
from app.dashboard.models.dashboard import Dashboard
from app.dashboard.schemas.dashboard import (
    DashboardSchema,
    DashboardCreate,
    DashboardUpdate,
)
from app.dashboard.services import dashboard_service
from app.analysis.services import analysis_service
from app.analysis.models.analysis import Analysis
from app.user.models.user import User
from app.analysis.schemas.analysis import AnalysisSchema

dashboard_router = APIRouter(prefix="/dashboard", tags=["Dashboards"])


def _empty_if_none(items):
    return items if items is not None else []


# ----------------------------------------------------------------------
#  LIST / PAGED
# ----------------------------------------------------------------------
@dashboard_router.get("/list", response_model=List[DashboardSchema])
def get_dashboards_list(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    dashboards = dashboard_service.get_page_dashboards_by_role_access(
        db, current_user, page, size, selected_entity_id
    )
    return _empty_if_none(dashboards)


@dashboard_router.get("/all", response_model=List[DashboardSchema])
def get_dashboards_all(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    dashboards = dashboard_service.get_dashboards_by_role_access(db, current_user, selected_entity_id)
    return _empty_if_none(dashboards)


@dashboard_router.get("/count")
def get_dashboard_count(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    return dashboard_service.get_dashboard_count_by_role_access(db, current_user, selected_entity_id)


# ----------------------------------------------------------------------
#  SINGLE DASHBOARD
# ----------------------------------------------------------------------
@dashboard_router.get("/{dashboard_id}", response_model=DashboardSchema)
def get_dashboard_by_id(
    dashboard_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    dashboard = dashboard_service.get_dashboard_by_id(db, dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
    if not dashboard_service.is_dashboard_accessible(db, current_user, dashboard, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return dashboard


# ----------------------------------------------------------------------
#  BY ANALYSIS ID
# ----------------------------------------------------------------------
@dashboard_router.get("/analysis/{analysis_id}/list", response_model=List[DashboardSchema])
def get_page_dashboards_by_analysis_id(
    analysis_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    analysis = analysis_service.get_analysis_by_id(db, analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail=f"Analysis {analysis_id} not found")
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    dashboards = dashboard_service.get_page_dashboards_by_analysis_id(
        db, analysis_id, page, size, selected_entity_id
    )
    return _empty_if_none(dashboards)


@dashboard_router.get("/analysis/{analysis_id}/all", response_model=List[DashboardSchema])
def get_all_dashboards_by_analysis_id(
    analysis_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    analysis = analysis_service.get_analysis_by_id(db, analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail=f"Analysis {analysis_id} not found")
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    dashboards = dashboard_service.get_all_dashboards_by_analysis_id(db, analysis_id, selected_entity_id)
    return _empty_if_none(dashboards)


@dashboard_router.get("/analysis/{analysis_id}/count")
def get_dashboard_count_by_analysis_id(
    analysis_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    analysis = analysis_service.get_analysis_by_id(db, analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail=f"Analysis {analysis_id} not found")
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return dashboard_service.get_dashboard_count_by_analysis_id(db, analysis_id, selected_entity_id)


# ----------------------------------------------------------------------
#  DELETE
# ----------------------------------------------------------------------
@dashboard_router.delete("/{dashboard_id}")
def delete_dashboard(
    dashboard_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    dashboard = dashboard_service.get_dashboard_by_id(db, dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
    if not dashboard_service.is_dashboard_accessible(db, current_user, dashboard, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    status = dashboard_service.delete_dashboard(db, dashboard_id)
    if status == 0:
        return {"message": "Dashboard deleted successfully"}
    raise HTTPException(status_code=404, detail=f"Could not delete dashboard {dashboard_id}")


# ----------------------------------------------------------------------
#  CREATE
# ----------------------------------------------------------------------
@dashboard_router.post("/create", response_model=DashboardSchema)
def dashboard_create(
    payload: DashboardCreate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    analysis = analysis_service.get_analysis_by_id(db, payload.analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail=f"Analysis {payload.analysis_id} not found")
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    dashboard = dashboard_service.create_dashboard(db, payload, selected_entity_id)
    if not dashboard:
        raise HTTPException(status_code=500, detail="Dashboard could not be created")
    return dashboard


# ----------------------------------------------------------------------
#  UPDATE
# ----------------------------------------------------------------------
@dashboard_router.put("/{dashboard_id}", response_model=DashboardSchema)
def update_dashboard(
    dashboard_id: int,
    payload: DashboardUpdate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    dashboard = dashboard_service.get_dashboard_by_id(db, dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
    if not dashboard_service.is_dashboard_accessible(db, current_user, dashboard, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    if payload.analysis_id is not None and payload.analysis_id != dashboard.analysis_id:
        new_analysis = analysis_service.get_analysis_by_id(db, payload.analysis_id)
        if not new_analysis:
            raise HTTPException(status_code=404, detail=f"Analysis {payload.analysis_id} not found")
        if not analysis_service.is_analysis_accessible(db, current_user, new_analysis, selected_entity_id):
            raise HTTPException(
                status_code=403, detail="Cannot move to analysis outside your entity"
            )

    updated = dashboard_service.update_dashboard(db, dashboard_id, payload, selected_entity_id)
    if not updated:
        raise HTTPException(status_code=500, detail="Update failed")
    return updated


# ----------------------------------------------------------------------
#  USER'S ANALYSES (dropdown)
# ----------------------------------------------------------------------
@dashboard_router.get("/user/analyses", response_model=List[AnalysisSchema])
def get_user_analyses_for_dashboard(
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    analyses = analysis_service.get_analyses_by_role_access(db, current_user, selected_entity_id)
    return _empty_if_none(analyses)