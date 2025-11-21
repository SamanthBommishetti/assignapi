# app/query/services/query_service.py
from sqlalchemy.orm import Session
from typing import List, Optional, Set
from app.query.models.query import Query as QueryTable
from app.query.schemas.query import QueryCreate
from app.queryresult.schemas.queryresult import QueryResultCreate
from app.queryresult.services.queryresult_service import create_queryresult
from app.context_table.services.context_table_service import get_context_table_by_id
from app.user.services.user_service import get_users_by_role_access
from app.analysis.services import analysis_service
from app.analysis.models.analysis import Analysis
from app.entity.model import Entity, EntityUserMap
import pandas as pd
import os
import re
import pytz
import clickhouse_connect

india_tz = pytz.timezone("Asia/Kolkata")

# Import and alias for consistency
from app.genai_tools.azure_text_to_sql import (
    genai_text_to_sql,
    azure_summarize_and_suggest_charts as genai_summarize_and_suggest_charts,
    CLICKHOUSE_CONFIG,
    parse_schema_columns,
    validate_sql_against_schema,
    TABLES,
)

def initialize_clickhouse_client():
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)


# ----------------------------------------------------------------------
#  ACCESS HELPERS
# ----------------------------------------------------------------------
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
def get_query_by_id(db: Session, query_id: int) -> QueryTable | None:
    return db.query(QueryTable).filter(QueryTable.query_id == query_id).first()


def get_page_queries_by_analysis_id(
    db: Session,
    analysis_id: int,
    page: int,
    size: int,
    selected_entity_id: Optional[int] = None,
) -> List[QueryTable]:
    offset = (page - 1) * size
    return (
        db.query(QueryTable)
        .filter(QueryTable.analysis_id == analysis_id)
        .offset(offset)
        .limit(size)
        .all()
    )


def get_all_queries_by_analysis_id(
    db: Session, analysis_id: int, selected_entity_id: Optional[int] = None
) -> List[QueryTable]:
    return db.query(QueryTable).filter(QueryTable.analysis_id == analysis_id).all()


def get_query_count_by_analysis_id(
    db: Session, analysis_id: int, selected_entity_id: Optional[int] = None
) -> int:
    return db.query(QueryTable).filter(QueryTable.analysis_id == analysis_id).count()


def get_page_queries_by_dashboard_id(
    db: Session,
    dashboard_id: int,
    page: int,
    size: int,
    selected_entity_id: Optional[int] = None,
) -> List[QueryTable]:
    offset = (page - 1) * size
    return (
        db.query(QueryTable)
        .filter(QueryTable.dashboard_id == dashboard_id)
        .offset(offset)
        .limit(size)
        .all()
    )


def get_all_queries_by_dashboard_id(
    db: Session, dashboard_id: int, selected_entity_id: Optional[int] = None
) -> List[QueryTable]:
    return db.query(QueryTable).filter(QueryTable.dashboard_id == dashboard_id).all()


def get_query_count_by_dashboard_id(
    db: Session, dashboard_id: int, selected_entity_id: Optional[int] = None
) -> int:
    return db.query(QueryTable).filter(QueryTable.dashboard_id == dashboard_id).count()


def get_page_queries_by_context_id(
    db: Session,
    context_id: int,
    page: int,
    size: int,
    selected_entity_id: Optional[int] = None,
) -> List[QueryTable]:
    offset = (page - 1) * size
    return (
        db.query(QueryTable)
        .filter(QueryTable.context_id == context_id)
        .offset(offset)
        .limit(size)
        .all()
    )


def get_all_queries_by_context_id(
    db: Session, context_id: int, selected_entity_id: Optional[int] = None
) -> List[QueryTable]:
    return db.query(QueryTable).filter(QueryTable.context_id == context_id).all()


def get_query_count_by_context_id(
    db: Session, context_id: int, selected_entity_id: Optional[int] = None
) -> int:
    return db.query(QueryTable).filter(QueryTable.context_id == context_id).count()


def create_query(db: Session, payload: QueryCreate) -> QueryTable:
    obj = QueryTable(
        query_str=payload.query_str,
        context_id=payload.context_id,
        analysis_id=payload.analysis_id,
        dashboard_id=payload.dashboard_id,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def delete_query_by_id(db: Session, query_id: int) -> int:
    obj = get_query_by_id(db, query_id)
    if obj:
        db.delete(obj)
        db.commit()
        return 0
    return -1


def update_query(
    db: Session, query_id: int, update_data: dict, selected_entity_id: Optional[int] = None
) -> QueryTable:
    obj = get_query_by_id(db, query_id)
    if not obj:
        return None
    for k, v in update_data.items():
        setattr(obj, k, v)
    db.commit()
    db.refresh(obj)
    return obj


# ----------------------------------------------------------------------
#  ROLE + ENTITY SCOPED
# ----------------------------------------------------------------------
def get_queries_by_role_access(
    db: Session, current_user, selected_entity_id: Optional[int] = None
) -> List[QueryTable]:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []
    return (
        db.query(QueryTable)
        .join(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .all()
    )


def get_page_queries_by_role_access(
    db: Session, current_user, page: int, size: int, selected_entity_id: Optional[int] = None
) -> List[QueryTable]:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []
    offset = (page - 1) * size
    return (
        db.query(QueryTable)
        .join(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .offset(offset)
        .limit(size)
        .all()
    )


def get_query_count_by_role_access(
    db: Session, current_user, selected_entity_id: Optional[int] = None
) -> int:
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return 0
    return (
        db.query(QueryTable)
        .join(Analysis)
        .join(EntityUserMap, Analysis.user_id == EntityUserMap.user_id)
        .filter(EntityUserMap.entity_id.in_(entity_ids))
        .count()
    )


def is_query_accessible(
    db: Session,
    current_user,
    query: QueryTable,
    selected_entity_id: Optional[int] = None,
) -> bool:
    if current_user.role.upper() == "SUPERADMIN":
        return True
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    analysis = analysis_service.get_analysis_by_id(db, query.analysis_id)
    return (
        db.query(EntityUserMap)
        .filter(
            EntityUserMap.user_id == analysis.user_id,
            EntityUserMap.entity_id.in_(entity_ids),
        )
        .first()
        is not None
    )


# ----------------------------------------------------------------------
#  GENAI (unchanged core)
# ----------------------------------------------------------------------
def get_context_name_by_id(db: Session, context_id: int) -> str:
    ctx = get_context_table_by_id(db, context_id)
    if not ctx:
        raise ValueError(f"Context {context_id} not found")
    raw = ctx.name.lower().strip()
    mapping = {
        "origination data": "origination",
        "performance data": "performance",
        "transactions data": "transactions",
    }
    return mapping.get(raw, raw)


def create_genai_query(
    db: Session, payload: QueryCreate, user_id: int, selected_entity_id: Optional[int] = None
):
    context_name = get_context_name_by_id(db, payload.context_id)
    if context_name not in TABLES:
        raise ValueError(f"Context '{context_name}' not supported")

    sql = genai_text_to_sql(payload.query_str, context_name)
    schema_cols = parse_schema_columns(TABLES[context_name]["schema"])
    is_valid, msg = validate_sql_against_schema(sql, schema_cols, TABLES[context_name]["table"])
    if not is_valid:
        raise ValueError(f"Invalid SQL: {msg}")

    client = initialize_clickhouse_client()
    result = client.query(sql)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    if df.empty:
        raise ValueError("No data returned")

    summary_text = genai_summarize_and_suggest_charts(df, payload.query_str)
    summary_match = re.search(r'###\s*Summary\s*\n(.*?)(?=\n###|\Z)', summary_text, re.DOTALL | re.IGNORECASE)
    summary_str = summary_match.group(1).strip() if summary_match else "No summary"

    chart_match = re.search(r'###\s*Main Chart\s*\n(.*?)(?=\n###|\Z)', summary_text, re.DOTALL | re.IGNORECASE)
    chart_type = chart_match.group(1).strip() if chart_match else "Table"

    suggested_match = re.search(r'###\s*Other Suggested Charts\s*\n(.*?)(?=\Z)', summary_text, re.DOTALL | re.IGNORECASE)
    suggested_text = suggested_match.group(1).strip() if suggested_match else ""
    suggested_charts = [line.strip('- *').strip() for line in suggested_text.split('\n') if line.strip()]
    suggested_charts_str = ','.join(suggested_charts[:2]) if suggested_charts else "Line,Bar"

    db_query = create_query(db, payload)
    query_id = db_query.query_id

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    out_dir = os.path.join(BASE_DIR, "out_csvfiles")
    os.makedirs(out_dir, exist_ok=True)

    filename = f"query{query_id}_user{user_id}_analysis{payload.analysis_id}_result.csv"
    out_csv_path = os.path.join(out_dir, filename)
    df.to_csv(out_csv_path, index=False)

    relative_csv_path = f"out_csvfiles/{filename}"
    result_create = QueryResultCreate(
        csv_file_name=relative_csv_path,
        chart_type=chart_type,
        summary=summary_str,
        suggested_charts=suggested_charts_str,
        query_id=query_id,
    )
    return create_queryresult(db, result_create)