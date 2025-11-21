# app/query/router.py
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from typing import List
from app.auth.services.auth_service import get_current_user
from app.core.database import get_db
from app.query.models.query import Query as QueryTable
from app.query.schemas.query import QuerySchema, QueryCreate, QueryUpdate
from app.query.services import query_service
from app.analysis.services import analysis_service
from app.dashboard.services import dashboard_service
from app.context_table.services import context_table_service
from app.user.models.user import User
from app.queryresult.schemas.queryresult import QueryResultSchema

query_router = APIRouter(prefix="/query", tags=["Queries"])


def _empty_if_none(items):
    return items if items is not None else []


# ----------------------------------------------------------------------
#  LIST / PAGED
# ----------------------------------------------------------------------
@query_router.get("/list", response_model=List[QuerySchema])
def get_queries_list(
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
    queries = query_service.get_page_queries_by_role_access(
        db, current_user, page, size, selected_entity_id
    )
    return _empty_if_none(queries)


@query_router.get("/all", response_model=List[QuerySchema])
def get_queries_all(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    queries = query_service.get_queries_by_role_access(db, current_user, selected_entity_id)
    return _empty_if_none(queries)


@query_router.get("/count")
def get_query_count(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    return query_service.get_query_count_by_role_access(db, current_user, selected_entity_id)


# ----------------------------------------------------------------------
#  SINGLE QUERY
# ----------------------------------------------------------------------
@query_router.get("/{query_id}", response_model=QuerySchema)
def get_query_by_id(
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
    if not query_service.is_query_accessible(db, current_user, query, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return query


# ----------------------------------------------------------------------
#  BY ANALYSIS / DASHBOARD / CONTEXT
# ----------------------------------------------------------------------
@query_router.get("/analysis/{analysis_id}/list", response_model=List[QuerySchema])
def get_page_queries_by_analysis_id(
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
    queries = query_service.get_page_queries_by_analysis_id(
        db, analysis_id, page, size, selected_entity_id
    )
    return _empty_if_none(queries)


@query_router.get("/analysis/{analysis_id}/all", response_model=List[QuerySchema])
def get_all_queries_by_analysis_id(
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
    queries = query_service.get_all_queries_by_analysis_id(db, analysis_id, selected_entity_id)
    return _empty_if_none(queries)


@query_router.get("/analysis/{analysis_id}/count")
def get_query_count_by_analysis_id(
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
    return query_service.get_query_count_by_analysis_id(db, analysis_id, selected_entity_id)


@query_router.get("/dashboard/{dashboard_id}/list", response_model=List[QuerySchema])
def get_page_queries_by_dashboard_id(
    dashboard_id: int,
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
    dashboard = dashboard_service.get_dashboard_by_id(db, dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
    if not dashboard_service.is_dashboard_accessible(db, current_user, dashboard, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    queries = query_service.get_page_queries_by_dashboard_id(
        db, dashboard_id, page, size, selected_entity_id
    )
    return _empty_if_none(queries)


@query_router.get("/dashboard/{dashboard_id}/all", response_model=List[QuerySchema])
def get_all_queries_by_dashboard_id(
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
    queries = query_service.get_all_queries_by_dashboard_id(db, dashboard_id, selected_entity_id)
    return _empty_if_none(queries)


@query_router.get("/dashboard/{dashboard_id}/count")
def get_query_count_by_dashboard_id(
    dashboard_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    dashboard = dashboard_service.get_dashboard_by_id(db, dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail=f"Dashboard {dashboard_id} not found")
    if not dashboard_service.is_dashboard_accessible(db, current_user, dashboard, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return query_service.get_query_count_by_dashboard_id(db, dashboard_id, selected_entity_id)


@query_router.get("/context/{context_id}/list", response_model=List[QuerySchema])
def get_page_queries_by_context_id(
    context_id: int,
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
    queries = query_service.get_page_queries_by_context_id(db, context_id, page, size, selected_entity_id)
    return [q for q in _empty_if_none(queries) if query_service.is_query_accessible(db, current_user, q, selected_entity_id)]


@query_router.get("/context/{context_id}/all", response_model=List[QuerySchema])
def get_all_queries_by_context_id(
    context_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    queries = query_service.get_all_queries_by_context_id(db, context_id, selected_entity_id)
    return [q for q in _empty_if_none(queries) if query_service.is_query_accessible(db, current_user, q, selected_entity_id)]


@query_router.get("/context/{context_id}/count")
def get_query_count_by_context_id(
    context_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    queries = query_service.get_all_queries_by_context_id(db, context_id, selected_entity_id)
    return len([q for q in _empty_if_none(queries) if query_service.is_query_accessible(db, current_user, q, selected_entity_id)])


# ----------------------------------------------------------------------
#  DELETE
# ----------------------------------------------------------------------
@query_router.delete("/{query_id}")
def delete_query_endpoint(
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
    if not query_service.is_query_accessible(db, current_user, query, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    status = query_service.delete_query_by_id(db, query_id)
    if status == 0:
        return {"message": "Query deleted successfully"}
    raise HTTPException(status_code=404, detail=f"Could not delete query {query_id}")


# ----------------------------------------------------------------------
#  CREATE (GenAI)
# ----------------------------------------------------------------------
@query_router.post("/create", response_model=QueryResultSchema)
def query_create(
    payload: QueryCreate,
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

    context = context_table_service.get_context_table_by_id(db, payload.context_id)
    if not context:
        raise HTTPException(status_code=404, detail=f"Context {payload.context_id} not found")

    dashboard = dashboard_service.get_dashboard_by_id(db, payload.dashboard_id)
    if not dashboard:
        raise HTTPException(status_code=404, detail=f"Dashboard {payload.dashboard_id} not found")
    if dashboard.analysis_id != payload.analysis_id:
        raise HTTPException(status_code=400, detail="Dashboard does not belong to the analysis")

    result = query_service.create_genai_query(db, payload, current_user.user_id, selected_entity_id)
    if not result:
        raise HTTPException(status_code=500, detail="Failed to create query")
    return result


# ----------------------------------------------------------------------
#  UPDATE
# ----------------------------------------------------------------------
@query_router.put("/{query_id}", response_model=QuerySchema)
def update_query(
    query_id: int,
    payload: QueryUpdate,
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
    if not query_service.is_query_accessible(db, current_user, query, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    update_data = {}
    if payload.query_str is not None:
        update_data["query_str"] = payload.query_str
    if payload.analysis_id is not None:
        new_analysis = analysis_service.get_analysis_by_id(db, payload.analysis_id)
        if not new_analysis:
            raise HTTPException(status_code=404, detail=f"Analysis {payload.analysis_id} not found")
        if not analysis_service.is_analysis_accessible(db, current_user, new_analysis, selected_entity_id):
            raise HTTPException(status_code=403, detail="Cannot move to analysis outside your entity")
        update_data["analysis_id"] = payload.analysis_id
    if payload.dashboard_id is not None:
        new_dashboard = dashboard_service.get_dashboard_by_id(db, payload.dashboard_id)
        if not new_dashboard:
            raise HTTPException(status_code=404, detail=f"Dashboard {payload.dashboard_id} not found")
        if not dashboard_service.is_dashboard_accessible(db, current_user, new_dashboard, selected_entity_id):
            raise HTTPException(status_code=403, detail="Cannot assign to dashboard outside your entity")
        update_data["dashboard_id"] = payload.dashboard_id
    if payload.context_id is not None:
        ctx = context_table_service.get_context_table_by_id(db, payload.context_id)
        if not ctx:
            raise HTTPException(status_code=404, detail=f"Context {payload.context_id} not found")
        update_data["context_id"] = payload.context_id

    updated = query_service.update_query(db, query_id, update_data, selected_entity_id)
    if not updated:
        raise HTTPException(status_code=500, detail="Update failed")
    return updated