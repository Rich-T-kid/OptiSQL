from pydantic import BaseModel, Field
from typing import Optional, Any

class HealthResponse(BaseModel):
    status: str = Field(..., description="Health status of the service")
    version: str = Field(default="0.1.0", description="API version")

class SQLQueryRequest(BaseModel):
    sql_query: str = Field(..., description="SQL query to process")

    class Config:
        json_schema_extra = {
            "example": {
                "sql_query": "SELECT * FROM users WHERE age > 25"
            }
        }

class SQLQueryResponse(BaseModel):
    status: str = Field(..., description="Processing status")
    query: str = Field(..., description="Original SQL query")
    result: Optional[Any] = Field(None, description="Query processing result")
    execution_time_ms: Optional[float] = Field(None, description="Execution time in milliseconds")

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "query": "SELECT * FROM users WHERE age > 25",
                "result": {"rows": 42},
                "execution_time_ms": 123.45
            }
        }
