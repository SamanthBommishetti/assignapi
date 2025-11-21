# app/queryresult/router.py
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from typing import List
from app.auth.services.auth_service import get_current_user
from app.core.database import get_db
from app.queryresult.schemas.queryresult import (
    QueryResultSchema,
    QueryResultCreate,
    QueryResultChartUpdate,
)
from app.queryresult.services import queryresult_service
from app.analysis.services import analysis_service
from app.query.services import query_service
from app.user.models.user import User
import pytz

india_tz = pytz.timezone('Asia/Kolkata')

queryresult_router = APIRouter(prefix='/queryresult', tags=['Query Results'])


@queryresult_router.get("/list", response_model=List[QueryResultSchema])
def get_queryresults_list(
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
    if current_user.role.upper() in ["SUPERADMIN", "ADMIN"]:
        results = queryresult_service.get_page_queryresults(db, page, size, selected_entity_id)
    else:
        results = queryresult_service.get_page_queryresults_by_user_id(
            db, current_user.user_id, page, size, selected_entity_id
        )
    if not results:
        raise HTTPException(status_code=404, detail="Query Results not found")
    return results


@queryresult_router.get("/all", response_model=List[QueryResultSchema])
def get_queryresults_list_all(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    if current_user.role.upper() in ["SUPERADMIN", "ADMIN"]:
        results = queryresult_service.get_all_queryresults(db, selected_entity_id)
    else:
        results = queryresult_service.get_queryresults_by_user_id(db, current_user.user_id, selected_entity_id)
    if not results:
        raise HTTPException(status_code=404, detail="Query Results not found")
    return results


@queryresult_router.get("/count")
def get_queryresult_count(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    if current_user.role.upper() in ["SUPERADMIN", "ADMIN"]:
        count = queryresult_service.get_queryresult_count(db, selected_entity_id)
    else:
        count = queryresult_service.get_queryresult_count_by_user_id(db, current_user.user_id, selected_entity_id)
    return count


@queryresult_router.get("/{result_id}", response_model=QueryResultSchema)
def get_queryresult_by_id(
    result_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    result = queryresult_service.get_queryresult_by_id(db, result_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Query Result {result_id} not found")
    query = query_service.get_query_by_id(db, result.query_id)
    analysis = analysis_service.get_analysis_by_id(db, query.analysis_id)
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return result


@queryresult_router.get('/query/{query_id}', response_model=QueryResultSchema)
def get_queryresult_by_query_id(
    query_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    query = query_service.get_query_by_id(db, query_id)
    if not query:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")
    analysis = analysis_service.get_analysis_by_id(db, query.analysis_id)
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    result = queryresult_service.get_queryresult_by_query_id(db, query_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Query Result for query {query_id} not found")
    return result


@queryresult_router.delete("/{result_id}")
def delete_queryresult(
    result_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    result = queryresult_service.get_queryresult_by_id(db, result_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Query Result {result_id} not found")
    query = query_service.get_query_by_id(db, result.query_id)
    analysis = analysis_service.get_analysis_by_id(db, query.analysis_id)
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    status = queryresult_service.delete_queryresult(db, result_id)
    if status == 0:
        return {"message": "Query Result deleted successfully"}
    raise HTTPException(status_code=404, detail=f"Could not delete result {result_id}")


@queryresult_router.post("/create", response_model=QueryResultSchema)
def queryresult_create(
    payload: QueryResultCreate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    query = query_service.get_query_by_id(db, payload.query_id)
    if not query:
        raise HTTPException(status_code=404, detail=f"Query {payload.query_id} not found")
    analysis = analysis_service.get_analysis_by_id(db, query.analysis_id)
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    result = queryresult_service.create_queryresult(db, payload)
    if not result:
        raise HTTPException(status_code=404, detail="Query Result could not be created")
    return result


@queryresult_router.put("/{result_id}/chart-type", response_model=QueryResultSchema)
def update_queryresult_chart_type(
    result_id: int,
    chart_update: QueryResultChartUpdate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    result = queryresult_service.get_queryresult_by_id(db, result_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Query Result {result_id} not found")
    query = query_service.get_query_by_id(db, result.query_id)
    analysis = analysis_service.get_analysis_by_id(db, query.analysis_id)
    if not analysis_service.is_analysis_accessible(db, current_user, analysis, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    updated = queryresult_service.update_chart_type(db, result_id, chart_update.chart_type)
    if not updated:
        raise HTTPException(status_code=404, detail="Failed to update chart type")
    return updated