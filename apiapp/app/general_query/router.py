# app/general_query/router.py
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.general_query.model import GeneralQuery
from app.navigation.model import Navigation, NavigationQueryMap
from app.general_query.schema import (
    GeneralQuerySchema,
    GeneralQueryCreate,
    BulkCreateResponse
)
from app.navigation.schema import NavigationSchema
from app.general_query import service as general_query_service
from app.auth.services.auth_service import get_current_user
from app.user.models.user import User
from app.user.services.user_service import get_users_by_role_access
from app.entity.model import EntityContextMap, EntityUserMap
from app.context_table.models.context_table import ContextNavigationMap


general_query_router = APIRouter(
    prefix="/general-query",
    tags=["General Queries"],
)

def _empty_if_none(items):
    return items if items is not None else []

# ----------------------------------------------------------------------
#  ACCESS UTILITIES
# ----------------------------------------------------------------------
def _accessible_user_ids(db: Session, current_user) -> set[int]:
    users = get_users_by_role_access(db, current_user)
    return {u.user_id for u in users}

# ----------------------------------------------------------------------
#  LIST / ALL – scoped (returns GeneralQuery objects)
# ----------------------------------------------------------------------
@general_query_router.get("/list", response_model=List[GeneralQuerySchema])
def get_general_queries(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    offset: int = Query(0, ge=0),
    size: int = Query(50, ge=1, le=100),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    queries = general_query_service.get_all_by_role_access(db, current_user, offset, size, selected_entity_id)
    return _empty_if_none(queries)


@general_query_router.get("/all", response_model=List[GeneralQuerySchema])
def get_all_general_queries(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    offset: int = Query(0, ge=0),
    size: int = Query(50, ge=1, le=100),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    queries = general_query_service.get_all_by_role_access(db, current_user, offset, size, selected_entity_id)
    return _empty_if_none(queries)

# ----------------------------------------------------------------------
#  SINGLE
# ----------------------------------------------------------------------
@general_query_router.get("/{query_id}", response_model=GeneralQuerySchema)
def get_general_query(
    query_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    query = general_query_service.get_by_id(db, query_id)
    if not query:
        raise HTTPException(status_code=404, detail=f"General Query {query_id} not found")
    if not general_query_service.is_query_accessible(db, current_user, query, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return query

# ----------------------------------------------------------------------
#  CREATE
# ----------------------------------------------------------------------
@general_query_router.post("/create", response_model=GeneralQuerySchema)
def create_general_query(
    payload: GeneralQueryCreate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    new_query = general_query_service.create(db, payload, current_user.user_id, selected_entity_id)
    if not new_query:
        raise HTTPException(status_code=500, detail="Failed to create general query")
    return new_query

# ----------------------------------------------------------------------
#  BULK CREATE (Property Type)
# ----------------------------------------------------------------------
@general_query_router.post("/create-property-type", response_model=BulkCreateResponse)
def create_property_type_navigation_endpoint(
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    result = general_query_service.create_property_type_navigation(db, current_user.user_id, selected_entity_id)
    return result

# ----------------------------------------------------------------------
#  NAVIGATION BY ID (single)
# ----------------------------------------------------------------------
@general_query_router.get("/navigation/{nav_id}", response_model=NavigationSchema)
def get_navigation(
    nav_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    nav = db.query(Navigation).filter(Navigation.navigation_id == nav_id).first()
    if not nav:
        raise HTTPException(status_code=404, detail=f"Navigation {nav_id} not found")
    # Add access check if needed
    if not general_query_service.is_navigation_accessible(db, current_user, nav, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return nav

# ----------------------------------------------------------------------
#  ALL QUERIES UNDER A NAVIGATION (FIXED: Returns GeneralQuery, not Navigation)
# ----------------------------------------------------------------------
@general_query_router.get("/navigation/{nav_id}/all", response_model=List[GeneralQuerySchema])
def get_all_queries_by_navigation_id(
    nav_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Returns ALL GeneralQuery objects linked to the given navigation_id,
    respecting role-based access control.
    """
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    queries = general_query_service.get_general_queries_by_navigation_id_and_role(db, nav_id, current_user, selected_entity_id)
    return _empty_if_none(queries)