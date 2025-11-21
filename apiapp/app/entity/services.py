# app/entity/services/entity_service.py
from sqlalchemy.orm import Session
from typing import List
from app.entity.model import Entity, EntityUserMap
from app.entity.schema import EntityCreate, EntityUpdate
from app.user.models.user import User

# ---------------- READ ------------------

def get_all_entities(db: Session):
    return db.query(Entity).all()

def get_page_entities(db: Session, page: int, size: int):
    offset = (page - 1) * size
    return db.query(Entity).offset(offset).limit(size).all()

def get_entity_count(db: Session) -> int:
    return db.query(Entity).count()

def get_entity_by_id(db: Session, entity_id: int):
    return db.query(Entity).filter(Entity.entity_id == entity_id).first()

def get_entity_by_name(db: Session, name: str):
    return db.query(Entity).filter(Entity.name == name).first()

# -------- GET USER ENTITIES --------

def get_user_entities(db: Session, user_id: int) -> List[Entity]:
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user:
        return []

    if user.role.upper() == "SUPERADMIN":
        return get_all_entities(db)

    primary_map = (
        db.query(EntityUserMap)
        .filter(EntityUserMap.user_id == user_id)
        .order_by(EntityUserMap.created_at)
        .first()
    )

    if not primary_map:
        return []

    primary_entity = get_entity_by_id(db, primary_map.entity_id)
    return [primary_entity] if primary_entity else []

# ---------------- CREATE ----------------

def create_entity(db: Session, entity: EntityCreate):
    db_entity = Entity(**entity.dict())
    db.add(db_entity)
    db.commit()
    db.refresh(db_entity)
    return db_entity

# ---------------- UPDATE ----------------

def update_entity(db: Session, entity_id: int, entity: EntityUpdate):
    db_entity = get_entity_by_id(db, entity_id)
    if not db_entity:
        return None

    update_data = entity.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_entity, key, value)

    db.commit()
    db.refresh(db_entity)
    return db_entity

# ---------------- DELETE ----------------

def delete_entity(db: Session, entity_id: int) -> bool:
    db_entity = get_entity_by_id(db, entity_id)
    if not db_entity:
        return False

    db.delete(db_entity)
    db.commit()
    return True
