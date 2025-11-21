# app/general_query/schema.py
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class GeneralQuerySchema(BaseModel):
    query_id: int
    query_sql_string: str
    query_str: str
    description: Optional[str] = None
    csv_file_path: Optional[str] = None
    chart_type: Optional[str] = None
    suggested_chart_types: Optional[str] = None
    summary: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class GeneralQueryCreate(BaseModel):
    query_sql_string: str
    query_str: str
    description: Optional[str] = None
    csv_file_path: Optional[str] = None
    chart_type: Optional[str] = None
    suggested_chart_types: Optional[str] = None
    summary: Optional[str] = None
    user_id: Optional[int] = None
    navigation_id: Optional[int] = None


class BulkCreateResponse(BaseModel):
    navigation: Optional[dict] = None
    queries: list[GeneralQuerySchema] = []
    message: str
