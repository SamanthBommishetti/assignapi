# app/general_query/service.py
from sqlalchemy.orm import Session
from app.general_query.model import GeneralQuery
from app.navigation.model import Navigation, NavigationQueryMap
from app.general_query.schema import GeneralQueryCreate
from app.navigation.schema import NavigationCreate
from app.user.models.user import User
from app.user.services.user_service import get_users_by_role_access

from app.entity.model import Entity, EntityContextMap, EntityUserMap
from app.context_table.models.context_table import ContextNavigationMap

import pandas as pd
from datetime import datetime
import clickhouse_connect
import os
import pytz
from typing import List, Optional, Set

# ----------------------------------------------------------------------
#  CLICKHOUSE CONFIG
# ----------------------------------------------------------------------
CLICKHOUSE_HOST = "57.159.27.80"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "fmac_db"
CLICKHOUSE_USER = "fmacadmin"
CLICKHOUSE_PASSWORD = "fmac*2025"
india_tz = pytz.timezone('Asia/Kolkata')

# ----------------------------------------------------------------------
#  ACCESS UTILITIES
# ----------------------------------------------------------------------
def _accessible_user_ids(db: Session, current_user) -> set[int]:
    """
    Returns the set of user IDs accessible to the given user
    based on role and entity mapping.
    """
    users = get_users_by_role_access(db, current_user)
    return {u.user_id for u in users}

def _accessible_entity_ids(db: Session, current_user) -> Set[int]:
    users = get_users_by_role_access(db, current_user)
    user_ids = {u.user_id for u in users}
    return {
        row[0] for row in db.query(EntityUserMap.entity_id)
        .filter(EntityUserMap.user_id.in_(user_ids))
        .all()
    }

def get_accessible_entity_ids(db: Session, current_user, selected_entity_id: Optional[int] = None) -> Set[int]:
    if current_user.role.upper() == "SUPERADMIN":
        if selected_entity_id == 0:
            return {e[0] for e in db.query(Entity).values(Entity.entity_id)}
        return {selected_entity_id}
    return _accessible_entity_ids(db, current_user)

def is_query_accessible(db: Session, current_user, query: GeneralQuery, selected_entity_id: Optional[int] = None) -> bool:
    if current_user.role.upper() == "SUPERADMIN":
        return True
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return False
    q = (
        db.query(NavigationQueryMap)
        .join(Navigation, NavigationQueryMap.navigation_id == Navigation.navigation_id)
        .join(ContextNavigationMap, Navigation.navigation_id == ContextNavigationMap.navigation_id)
        .join(EntityContextMap, ContextNavigationMap.context_id == EntityContextMap.context_id)
        .filter(
            NavigationQueryMap.query_id == query.query_id,
            EntityContextMap.entity_id.in_(entity_ids)
        )
    )
    if current_user.role.upper() != "SUPERADMIN":
        allowed = _accessible_user_ids(db, current_user)
        q = q.join(EntityUserMap, EntityContextMap.entity_id == EntityUserMap.entity_id) \
             .filter(EntityUserMap.user_id.in_(allowed))
    return q.first() is not None

# ----------------------------------------------------------------------
#  BASIC CRUD OPERATIONS
# ----------------------------------------------------------------------
def get_by_id(db: Session, query_id: int) -> GeneralQuery | None:
    """Fetch a GeneralQuery by its unique ID."""
    return db.query(GeneralQuery).filter(GeneralQuery.query_id == query_id).first()


def create(db: Session, query: GeneralQueryCreate, user_id: int) -> GeneralQuery:
    """
    Create a new GeneralQuery record and link it to a Navigation (if provided).
    Note: user_id and navigation_id are not stored in GeneralQuery directly anymore.
    """
    db_query = GeneralQuery(**query.dict(exclude={'user_id', 'navigation_id'}))
    db.add(db_query)
    db.commit()
    db.refresh(db_query)

    # Create mapping with Navigation if provided
    if hasattr(query, 'navigation_id') and query.navigation_id:
        nav_map = NavigationQueryMap(
            navigation_id=query.navigation_id,
            query_id=db_query.query_id
        )
        db.add(nav_map)
        db.commit()

    return db_query

# ----------------------------------------------------------------------
#  ROLE-SCOPED QUERIES (return GeneralQuery objects)
# ----------------------------------------------------------------------
def get_all_by_role_access(db: Session, current_user, offset: int = 0, size: int = 50, selected_entity_id: Optional[int] = None):
    """
    Return GeneralQuery objects accessible to the user.
    """
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []

    q = (
        db.query(GeneralQuery)
        .join(
            NavigationQueryMap,
            GeneralQuery.query_id == NavigationQueryMap.query_id
        )
        .join(
            Navigation,
            NavigationQueryMap.navigation_id == Navigation.navigation_id
        )
        .join(
            ContextNavigationMap,
            Navigation.navigation_id == ContextNavigationMap.navigation_id
        )
        .join(
            EntityContextMap,
            ContextNavigationMap.context_id == EntityContextMap.context_id
        )
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .distinct(GeneralQuery.query_id)
    )
    if current_user.role.upper() != "SUPERADMIN":
        allowed = _accessible_user_ids(db, current_user)
        q = q.join(
            EntityUserMap,
            EntityContextMap.entity_id == EntityUserMap.entity_id
        ) \
        .filter(EntityUserMap.user_id.in_(allowed))
    return q.offset(offset).limit(size).all()

def get_general_queries_by_navigation_id_and_role(db: Session, nav_id: int, current_user: User, selected_entity_id: Optional[int] = None):
    """
    Fetch all GeneralQuery objects linked to a specific Navigation ID,
    filtered by the current user's role access.
    """
    entity_ids = get_accessible_entity_ids(db, current_user, selected_entity_id)
    if not entity_ids:
        return []

    q = (
        db.query(GeneralQuery)
        .join(
            NavigationQueryMap,
            GeneralQuery.query_id == NavigationQueryMap.query_id
        )
        .filter(NavigationQueryMap.navigation_id == nav_id)
        .join(
            Navigation,
            NavigationQueryMap.navigation_id == Navigation.navigation_id
        )
        .join(
            ContextNavigationMap,
            Navigation.navigation_id == ContextNavigationMap.navigation_id
        )
        .join(
            EntityContextMap,
            ContextNavigationMap.context_id == EntityContextMap.context_id
        )
        .filter(EntityContextMap.entity_id.in_(entity_ids))
        .distinct(GeneralQuery.query_id)
    )
    if current_user.role.upper() != "SUPERADMIN":
        allowed = _accessible_user_ids(db, current_user)
        q = q.join(
            EntityUserMap,
            EntityContextMap.entity_id == EntityUserMap.entity_id
        ) \
        .filter(EntityUserMap.user_id.in_(allowed))
    return q.all()

# ----------------------------------------------------------------------
#  PROPERTY TYPE NAVIGATION (Example Bulk Create)
# ----------------------------------------------------------------------
def create_property_type_navigation(db: Session, user_id: int):
    """
    Example function to generate a Navigation entry and its related
    GeneralQuery items (for 'Property Type' analytics).
    """
    nav_title = "Property Type"
    nav_stem = "/property_type"
    nav_desc = "Analysis of loans by property type, state, and related metrics from origination data."
    context_id = 1  # Replace with actual context_id for 'Origination'

    nav_create = NavigationCreate(
        navigation_title=nav_title,
        navigation_stem=nav_stem,
        navigation_description=nav_desc,
        context_id=context_id
    )

    navigation = create_navigation(db, nav_create)
    navigation_id = navigation.navigation_id

    client = _initialize_clickhouse_client()
    output_dir = "general_out_csvfiles/propertytype"
    os.makedirs(output_dir, exist_ok=True)

    queries = [
        (
            "loans_by_state",
            """
            SELECT property_state, COUNT(*) AS total_loans
            FROM origination
            GROUP BY property_state
            ORDER BY total_loans DESC
            """,
            "How many loans exist by property state?",
            "Loans count by state",
            "Bar Chart",
            "Choropleth Map"
        ),
    ]

    created_queries = []
    for i, (name, sql, q_str, desc, chart, suggested_chart) in enumerate(queries, start=1):
        try:
            result = client.query(sql)
            result_df = pd.DataFrame(result.result_rows, columns=result.column_names)
            csv_filename = f"general_query_{i}_{name}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            result_df.to_csv(csv_path, index=False, encoding="utf-8")

            query_create = GeneralQueryCreate(
                query_sql_string=sql.strip(),
                query_str=q_str,
                description=desc,
                csv_file_path=f"{output_dir}/{csv_filename}",
                chart_type=chart,
                suggested_chart_types=suggested_chart,
                summary=None
            )
            new_query = create(db, query_create, user_id)
            created_queries.append(new_query)
        except Exception as e:
            print(f"Failed to process query {i}: {e}")
            continue

    client.close()
    return {
        'navigation': navigation,
        'queries': created_queries,
        'message': f'Successfully created/updated Property Type navigation with {len(created_queries)} queries.'
    }

# ----------------------------------------------------------------------
#  NAVIGATION CREATION HELPER
# ----------------------------------------------------------------------
def create_navigation(db: Session, nav: NavigationCreate):
    """Create or fetch existing Navigation record."""
    existing = db.query(Navigation).filter(Navigation.navigation_title == nav.navigation_title).first()
    if existing:
        return existing
    db_nav = Navigation(**nav.dict())
    db.add(db_nav)
    db.commit()
    db.refresh(db_nav)
    return db_nav


# ----------------------------------------------------------------------
#  CLICKHOUSE CLIENT INITIALIZATION
# ----------------------------------------------------------------------
def _initialize_clickhouse_client():
    """Initialize ClickHouse client with proper error handling."""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        return client
    except Exception as e:
        print(f"Failed to initialize ClickHouse client: {e}")
        raise