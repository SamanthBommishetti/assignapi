# app/navigation/router.py
from fastapi import APIRouter, Query, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.navigation.schema import NavigationSchema, NavigationCreate, NavigationUpdate
from app.navigation import service as navigation_service 
from app.auth.services.auth_service import get_current_user
from app.user.models.user import User

navigation_router = APIRouter(
    prefix="/Navigations",
    tags=["Navigations"]
)

# ----------------------------------------------------------------------
# LIST (paginated)
# ----------------------------------------------------------------------
@navigation_router.get("/list", response_model=List[NavigationSchema])
def get_navigations_list(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),  
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1)
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    navigations = navigation_service.get_page_navigations_by_role_access(db, current_user, page, size, selected_entity_id)
    if not navigations:
        raise HTTPException(status_code=404, detail="No navigations found")
    return navigations

# ----------------------------------------------------------------------
# ALL
# ----------------------------------------------------------------------
@navigation_router.get("/all", response_model=List[NavigationSchema])
def get_all_navigations_endpoint(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    navigations = navigation_service.get_all_by_role_access(db, current_user, selected_entity_id)
    if not navigations:
        raise HTTPException(status_code=404, detail="No navigations found")
    return navigations

# ----------------------------------------------------------------------
# COUNT
# ----------------------------------------------------------------------
@navigation_router.get("/count")
def get_navigations_count_endpoint(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    return navigation_service.get_navigation_count_by_role_access(db, current_user, selected_entity_id)

# ----------------------------------------------------------------------
# GET BY ID
# ----------------------------------------------------------------------
@navigation_router.get("/{navigation_id}", response_model=NavigationSchema)
def get_navigation_by_id_endpoint(
    navigation_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    nav = navigation_service.get_by_id(db, navigation_id)
    if not nav:
        raise HTTPException(status_code=404, detail=f"Navigation {navigation_id} not found")
    if not navigation_service.is_navigation_accessible(db, current_user, nav, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return nav

# ----------------------------------------------------------------------
# GET ALL BY CONTEXT ID
# ----------------------------------------------------------------------
@navigation_router.get("/context/{context_id}/all", response_model=List[NavigationSchema])
def get_all_navigations_by_context_id(
    context_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    navigations = navigation_service.get_navigations_by_context_id_and_role(db, context_id, current_user, selected_entity_id)
    # Return empty list if none, instead of 404 to avoid issues
    return navigations or []

# ----------------------------------------------------------------------
# DELETE BY ID
# ----------------------------------------------------------------------
@navigation_router.delete("/{navigation_id}")
def delete_navigation_endpoint(
    navigation_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    success = navigation_service.delete(db, navigation_id, selected_entity_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Navigation with id {navigation_id} not found")
    return {"message": f"Navigation {navigation_id} deleted successfully"}

# ----------------------------------------------------------------------
# CREATE
# ----------------------------------------------------------------------
@navigation_router.post("/create", response_model=NavigationSchema)
def create_navigation_endpoint(
    nav: NavigationCreate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    if navigation_service.get_by_stem(db, nav.navigation_stem, selected_entity_id):
        raise HTTPException(status_code=400, detail="Navigation stem already exists")
    new_nav = navigation_service.create(db, nav, selected_entity_id)
    return new_nav

# ----------------------------------------------------------------------
# UPDATE (PUT)
# ----------------------------------------------------------------------
@navigation_router.put("/{navigation_id}", response_model=NavigationSchema)
def update_navigation_endpoint(
    navigation_id: int,
    nav: NavigationUpdate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    updated = navigation_service.update(db, navigation_id, nav, selected_entity_id)
    if not updated:
        raise HTTPException(status_code=404, detail=f"Navigation with id {navigation_id} not found")
    return updated