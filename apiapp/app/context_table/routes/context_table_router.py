# app/context_table/router.py
from fastapi import APIRouter, Query, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.context_table.models.context_table import ContextTable
from app.context_table.schemas.context_table import (
    ContextTableSchema, ContextTableCreate, ContextTableUpdate
)
from app.context_table.services import context_table_service
from app.auth.services.auth_service import get_current_user
from app.user.models.user import User

context_table_router = APIRouter(
    prefix="/contexttable",
    tags=["Context Tables"]
)

# ----------------------------------------------------------------------
#  LIST (paginated) – role-scoped
# ----------------------------------------------------------------------
@context_table_router.get("/list", response_model=List[ContextTableSchema])
def get_context_tables_list(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    tables = context_table_service.get_page_context_tables_by_role(db, current_user, page, size, selected_entity_id)
    return tables  # Return empty list if none found


# ----------------------------------------------------------------------
#  ALL – role-scoped
# ----------------------------------------------------------------------
@context_table_router.get("/all", response_model=List[ContextTableSchema])
def get_all_context_tables_endpoint(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    tables = context_table_service.get_all_context_tables_by_role(db, current_user, selected_entity_id)
    return tables  # Return empty list if none found


# ----------------------------------------------------------------------
#  COUNT – role-scoped
# ----------------------------------------------------------------------
@context_table_router.get("/count")
def get_context_tables_count_endpoint(
    entity_id: int = Query(None, description="Entity ID to filter by (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> int:
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    return context_table_service.get_context_tables_count_by_role(db, current_user, selected_entity_id)


# ----------------------------------------------------------------------
#  GET BY ID – with access check
# ----------------------------------------------------------------------
@context_table_router.get("/{context_id}", response_model=ContextTableSchema)
def get_context_table_by_id_endpoint(
    context_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    ctx = context_table_service.get_context_table_by_id(db, context_id)
    if not ctx:
        raise HTTPException(status_code=404, detail=f"ContextTable {context_id} not found")
    if not context_table_service.is_context_accessible(db, current_user, ctx, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return ctx

# ----------------------------------------------------------------------
#  DELETE BY ID
# ----------------------------------------------------------------------
@context_table_router.delete("/{context_id}")
def delete_context_table_endpoint(
    context_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    ctx = context_table_service.get_context_table_by_id(db, context_id)
    if not ctx:
        raise HTTPException(status_code=404, detail=f"ContextTable {context_id} not found")
    if not context_table_service.is_context_accessible(db, current_user, ctx, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    success = context_table_service.delete_context_table(db, context_id)
    if not success:
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"message": f"ContextTable {context_id} deleted successfully"}

# ----------------------------------------------------------------------
#  CREATE
# ----------------------------------------------------------------------
@context_table_router.post("/create", response_model=ContextTableSchema)
def create_context_table_endpoint(
    ctx: ContextTableCreate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    if context_table_service.get_context_table_by_name(db, ctx.name):
        raise HTTPException(status_code=400, detail="Context table name already exists")
    ctx.user_id = current_user.user_id
    new_ctx = context_table_service.create_context_table(db, ctx, selected_entity_id)
    return new_ctx

# ----------------------------------------------------------------------
#  UPDATE
# ----------------------------------------------------------------------
@context_table_router.put("/{context_id}", response_model=ContextTableSchema)
def update_context_table_endpoint(
    context_id: int,
    ctx: ContextTableUpdate,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    existing = context_table_service.get_context_table_by_id(db, context_id)
    if not existing:
        raise HTTPException(status_code=404, detail=f"ContextTable {context_id} not found")
    if not context_table_service.is_context_accessible(db, current_user, existing, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    if ctx.user_id is not None and current_user.role.upper() != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="Cannot change owner")
    updated = context_table_service.update_context_table(db, context_id, ctx, selected_entity_id)
    return updated

# ----------------------------------------------------------------------
#  FULL BY ID (with relationships) – optional
# ----------------------------------------------------------------------
@context_table_router.get("/{context_id}/all", response_model=ContextTableSchema)
def get_full_context_table_by_id_endpoint(
    context_id: int,
    entity_id: int = Query(None, description="Entity ID for access check (0 for all)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    is_super = current_user.role.upper() == "SUPERADMIN"
    selected_entity_id = entity_id if is_super else None
    if is_super and entity_id is None:
        selected_entity_id = 0
    ctx = context_table_service.get_context_table_by_id(db, context_id)
    if not ctx:
        raise HTTPException(status_code=404, detail=f"ContextTable {context_id} not found")
    if not context_table_service.is_context_accessible(db, current_user, ctx, selected_entity_id):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return ctx