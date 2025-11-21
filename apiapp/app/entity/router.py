# app/entity/router.py
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.entity.schema import (
    EntitySchema, EntityCreate, EntityUpdate
)
from app.entity.services import (
    get_all_entities, get_page_entities, get_entity_count,
    get_entity_by_id, get_entity_by_name, create_entity,
    update_entity, delete_entity, get_user_entities
)
from app.auth.services.auth_service import get_current_user
from app.user.models.user import User

entity_router = APIRouter(
    prefix="/entity",
    tags=["Entities"]
)

# ----------------------------------------------------------------------
# LIST (paginated)
# ----------------------------------------------------------------------
@entity_router.get("/list", response_model=List[EntitySchema])
def get_entity_list(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN":
        return []
    return get_page_entities(db, page, size)

# ----------------------------------------------------------------------
# ALL ENTITIES
# ----------------------------------------------------------------------
@entity_router.get("/all", response_model=List[EntitySchema])
def get_all_entity_list(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN":
        return []
    return get_all_entities(db)

# ----------------------------------------------------------------------
# COUNT
# ----------------------------------------------------------------------
@entity_router.get("/count")
def get_entity_count_endpoint(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> int:
    if current_user.role.upper() != "SUPERADMIN":
        return 0
    return get_entity_count(db)

# ----------------------------------------------------------------------
# GET ENTITY BY ID
# ----------------------------------------------------------------------
@entity_router.get("/{entity_id}", response_model=EntitySchema)
def get_entity_by_id_endpoint(
    entity_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="SUPERADMIN only")

    entity = get_entity_by_id(db, entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity

# ----------------------------------------------------------------------
# DELETE ENTITY
# ----------------------------------------------------------------------
@entity_router.delete("/{entity_id}")
def delete_entity_endpoint(
    entity_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="SUPERADMIN only")

    deleted = delete_entity(db, entity_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Entity not found")

    return {"message": f"Entity {entity_id} deleted"}

# ----------------------------------------------------------------------
# CREATE ENTITY
# ----------------------------------------------------------------------
@entity_router.post("/create", response_model=EntitySchema)
def create_entity_endpoint(
    entity: EntityCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="SUPERADMIN only")

    if get_entity_by_name(db, entity.name):
        raise HTTPException(status_code=400, detail="Entity name exists")

    return create_entity(db, entity)

# ----------------------------------------------------------------------
# UPDATE ENTITY
# ----------------------------------------------------------------------
@entity_router.put("/{entity_id}", response_model=EntitySchema)
def update_entity_endpoint(
    entity_id: int,
    entity: EntityUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="SUPERADMIN only")

    updated = update_entity(db, entity_id, entity)
    if not updated:
        raise HTTPException(status_code=404, detail="Entity not found")

    return updated

# ----------------------------------------------------------------------
# GET USER'S ENTITIES
# ----------------------------------------------------------------------
@entity_router.get("/user/{user_id}/entities", response_model=List[EntitySchema])
def get_user_entities_endpoint(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN" and current_user.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

    return get_user_entities(db, user_id)

# ----------------------------------------------------------------------
# SWITCH SELECTED ENTITY ( *** FIXED *** )
# ----------------------------------------------------------------------
@entity_router.post("/{entity_id}/switch", response_model=dict)
def switch_selected_entity_endpoint(
    entity_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role.upper() != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="SUPERADMIN only")

    # Switch to ALL
    if entity_id == 0:
        request.session["selected_entity_id"] = 0
        request.session["selected_entity_name"] = "All Entities"
        return {"message": "Switched to All Entities", "entity_id": 0, "entity_name": "All Entities"}

    entity = get_entity_by_id(db, entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    # Store in session
    request.session["selected_entity_id"] = entity.entity_id
    request.session["selected_entity_name"] = entity.name

    return {
        "message": f"Switched to {entity.name}",
        "entity_id": entity.entity_id,
        "entity_name": entity.name
    }
