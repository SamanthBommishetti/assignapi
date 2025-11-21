# app/queryresult/services/queryresult_service.py
from sqlalchemy.orm import Session
from typing import List, Optional, Set
from app.queryresult.models.queryresult import QueryResult
from app.queryresult.schemas.queryresult import QueryResultCreate
from app.query.models.query import Query
from app.analysis.models.analysis import Analysis
from app.entity.model import Entity, EntityUserMap
from app.user.services.user_service import get_users_by_role_access


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
def get_all_queryresults(db: Session, selected_entity_id: Optional[int] = None):
    return db.query(QueryResult).all()


def get_page_queryresults(
    db: Session, page: int, size: int, selected_entity_id: Optional[int] = None
):
    offset = (page - 1) * size
    return db.query(QueryResult).offset(offset).limit(size).all()


def get_queryresult_by_id(db: Session, result_id: int):
    return db.query(QueryResult).filter(QueryResult.result_id == result_id).first()


def get_queryresult_by_query_id(db: Session, query_id: int):
    return db.query(QueryResult).filter(QueryResult.query_id == query_id).first()


def get_queryresult_count(db: Session, selected_entity_id: Optional[int] = None) -> int:
    return db.query(QueryResult).count()


def create_queryresult(db: Session, payload: QueryResultCreate):
    db_obj = QueryResult(
        csv_file_name=payload.csv_file_name,
        chart_type=payload.chart_type,
        summary=payload.summary,
        suggested_charts=','.join(payload.suggested_charts),
        query_id=payload.query_id,
    )
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj


def delete_queryresult(db: Session, result_id: int):
    obj = get_queryresult_by_id(db, result_id)
    if obj:
        db.delete(obj)
        db.commit()
        return 0
    return -1


def update_chart_type(db: Session, result_id: int, chart_type: str):
    obj = get_queryresult_by_id(db, result_id)
    if not obj:
        return None
    obj.chart_type = chart_type
    db.commit()
    db.refresh(obj)
    return obj


# ----------------------------------------------------------------------
#  ROLE + ENTITY SCOPED
# ----------------------------------------------------------------------
def get_queryresults_by_user_id(
    db: Session, user_id: int, selected_entity_id: Optional[int] = None
) -> List[QueryResult]:
    return (
        db.query(QueryResult)
        .join(Query)
        .join(Analysis)
        .filter(Analysis.user_id == user_id)
        .all()
    )


def get_page_queryresults_by_user_id(
    db: Session, user_id: int, page: int, size: int, selected_entity_id: Optional[int] = None
):
    offset = (page - 1) * size
    return (
        db.query(QueryResult)
        .join(Query)
        .join(Analysis)
        .filter(Analysis.user_id == user_id)
        .offset(offset)
        .limit(size)
        .all()
    )


def get_queryresult_count_by_user_id(
    db: Session, user_id: int, selected_entity_id: Optional[int] = None
) -> int:
    return (
        db.query(QueryResult)
        .join(Query)
        .join(Analysis)
        .filter(Analysis.user_id == user_id)
        .count()
    )