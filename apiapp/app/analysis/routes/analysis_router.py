# app/analysis/router.py
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from typing import List
from app.auth.services.auth_service import get_current_user
from app.core.database import get_db
from app.analysis.models.analysis import Analysis
from app.analysis.schemas.analysis import AnalysisSchema, AnalysisCreate
from app.analysis.services import analysis_service
from app.user.models.user import User

analysis_router = APIRouter(prefix="/analysis", tags=["Analysis"])


def _empty_if_none(items):
    return items if items is not None else []


# ----------------------------------------------------------------------
#  LIST / PAGED (role + entity scoped)
# ----------------------------------------------------------------------
@analysis_router.get("/list", response_model=List[AnalysisSchema])
def get_analyses_list(
    req: Request,
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
    analyses = analysis_service.get_page_analyses_by_role_access(
        db, current_selection=selected_entity_id, user=current_user, page=page, size=size
    )
    return _empty_if_none(analyses)


@analysis_router.get("/all", response_model=List[AnalysisSchema])
def get_analyses_all(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    analyses = analysis_service.get_analyses_by_role_access(
        db, current_selection=selected_entity_id, user=current_user
    )
    return _empty_if_none(analyses)


@analysis_router.get("/count")
def get_analysis_count(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    return analysis_service.get_analysis_count_by_role_access(
        db, current_selection=selected_entity_id, user=current_user
    )


# ----------------------------------------------------------------------
#  SINGLE ANALYSIS
# ----------------------------------------------------------------------
@analysis_router.get("/{analysis_id}", response_model=AnalysisSchema)
def get_analysis_by_id(
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

    if not analysis_service.is_analysis_accessible(
        db, current_user, analysis, selected_entity_id
    ):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return analysis


# ----------------------------------------------------------------------
#  BY USER ID
# ----------------------------------------------------------------------
@analysis_router.get("/user/{user_id}/list", response_model=List[AnalysisSchema])
def get_page_analyses_by_user_id(
    user_id: int,
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
    if not analysis_service.can_see_user_analyses(
        db, current_user, user_id, selected_entity_id
    ):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    analyses = analysis_service.get_page_analyses_by_user_id(
        db, user_id, page, size, selected_entity_id
    )
    return _empty_if_none(analyses)


@analysis_router.get("/user/{user_id}/all", response_model=List[AnalysisSchema])
def get_all_analyses_by_user_id(
    user_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    if not analysis_service.can_see_user_analyses(
        db, current_user, user_id, selected_entity_id
    ):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    analyses = analysis_service.get_all_analyses_by_user_id(db, user_id, selected_entity_id)
    return _empty_if_none(analyses)


@analysis_router.get("/user/{user_id}/count")
def get_analysis_by_user_id_count(
    user_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    if not analysis_service.can_see_user_analyses(
        db, current_user, user_id, selected_entity_id
    ):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return analysis_service.get_analysis_by_user_id_count(db, user_id, selected_entity_id)


# ----------------------------------------------------------------------
#  DELETE
# ----------------------------------------------------------------------
@analysis_router.delete("/{analysis_id}")
def delete_analysis(
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
    if not analysis_service.is_analysis_accessible(
        db, current_user, analysis, selected_entity_id
    ):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    status = analysis_service.delete_analysis(db, analysis_id)
    if status == 0:
        return {"message": "Analysis deleted successfully"}
    raise HTTPException(status_code=404, detail=f"Could not delete analysis {analysis_id}")


# ----------------------------------------------------------------------
#  CREATE
# ----------------------------------------------------------------------
@analysis_router.post("/create", response_model=AnalysisSchema)
def analysis_create(
    payload: AnalysisCreate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    if current_user.role.upper() == "USER" and payload.user_id != current_user.user_id:
        raise HTTPException(status_code=403, detail="Users can only create for themselves")

    if current_user.role.upper() in ["SUPERADMIN", "ADMIN"]:
        if not analysis_service.can_see_user_analyses(
            db, current_user, payload.user_id, selected_entity_id
        ):
            raise HTTPException(status_code=403, detail="Cannot create for a user outside your entity")

    analysis = analysis_service.create_analysis(
        db, payload, payload.user_id or current_user.user_id, selected_entity_id
    )
    if not analysis:
        raise HTTPException(status_code=500, detail="Analysis could not be created")
    return analysis
