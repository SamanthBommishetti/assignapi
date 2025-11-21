from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import List, Optional, Union

class QueryResultBase(BaseModel):
    csv_file_name: str
    chart_type: str
    summary: str
    query_id: int
    suggested_charts: List[str]  # API uses list; DB stores as comma-str

    @field_validator("suggested_charts", mode="before")
    @classmethod
    def split_suggested_charts(cls, v: Union[str, List[str], None]) -> List[str]:
        """
        Convert the DB's comma-separated string into a list for the API response.
        For input, accepts list or str (joins if list).
        """
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return [x.strip() for x in str(v).split(",") if x.strip()]

class QueryResultCreate(QueryResultBase):
    pass

class QueryResultSchema(QueryResultBase):
    result_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {  # Pydantic v2
        "from_attributes": True,
    }

class QueryResultChartUpdate(BaseModel):
    chart_type: str

    @field_validator("chart_type")
    @classmethod
    def validate_chart_type(cls, v):
        if not v or not v.strip():
            raise ValueError("Chart type cannot be empty")
        return v.strip()