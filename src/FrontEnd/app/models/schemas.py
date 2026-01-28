from pydantic import BaseModel, Field
from typing import Optional, Any, Dict, List
from enum import Enum


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


class ParseErrorDetail(BaseModel):
    message: str = Field(..., description="Error message")
    line: Optional[int] = Field(None, description="Line number where error occurred")
    column: Optional[int] = Field(None, description="Column number where error occurred")


class ParseResultModel(BaseModel):
    success: bool = Field(..., description="Whether parsing succeeded")
    ast: Optional[Dict[str, Any]] = Field(None, description="Abstract Syntax Tree of the SQL query")
    optimized: bool = Field(default=False, description="Whether optimizations were applied")
    error: Optional[ParseErrorDetail] = Field(None, description="Parse error details if parsing failed")


class QueryStatus(str, Enum):
    SUCCESS = "success"
    PARSE_ERROR = "parse_error"
    EXECUTION_ERROR = "execution_error"
    SOURCE_ERROR = "source_error"
    ERROR = "error"


class SQLQueryResponse(BaseModel):
    status: QueryStatus = Field(..., description="Processing status")
    query: str = Field(..., description="Original SQL query")
    parse_result: Optional[ParseResultModel] = Field(None, description="SQL parsing result with AST")
    result: Optional[Any] = Field(None, description="Query execution result")
    execution_time_ms: Optional[float] = Field(None, description="Execution time in milliseconds")
    error_message: Optional[str] = Field(None, description="Error message if status is not success")

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "query": "SELECT name, age FROM users WHERE age > 25",
                "parse_result": {
                    "success": True,
                    "ast": {
                        "type": "SelectStatement",
                        "select": {
                            "distinct": False,
                            "items": [
                                {"expression": {"type": "Identifier", "parts": ["name"]}},
                                {"expression": {"type": "Identifier", "parts": ["age"]}}
                            ]
                        },
                        "from": {
                            "primary": {"type": "BaseTable", "name": "users"}
                        },
                        "where": {
                            "type": "BinaryExpr",
                            "operator": ">",
                            "left": {"type": "Identifier", "parts": ["age"]},
                            "right": {"type": "Literal", "dataType": "integer", "value": 25}
                        }
                    },
                    "optimized": True
                },
                "result": {"rows_affected": 42},
                "execution_time_ms": 123.45
            }
        }


class ParseOnlyRequest(BaseModel):
    sql_query: str = Field(..., description="SQL query to parse")
    optimize: bool = Field(default=True, description="Whether to apply optimizations")

    class Config:
        json_schema_extra = {
            "example": {
                "sql_query": "SELECT 1 + 1, name FROM users WHERE active = TRUE",
                "optimize": True
            }
        }


class ParseOnlyResponse(BaseModel):
    success: bool = Field(..., description="Whether parsing succeeded")
    sql_query: str = Field(..., description="Original SQL query")
    ast: Optional[Dict[str, Any]] = Field(None, description="Abstract Syntax Tree")
    optimized: bool = Field(default=False, description="Whether optimizations were applied")
    error: Optional[ParseErrorDetail] = Field(None, description="Parse error details")
    parse_time_ms: float = Field(..., description="Time taken to parse in milliseconds")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "sql_query": "SELECT 1 + 1, name FROM users WHERE active = TRUE",
                "ast": {
                    "type": "SelectStatement",
                    "select": {
                        "items": [
                            {"expression": {"type": "Literal", "dataType": "integer", "value": 2}},
                            {"expression": {"type": "Identifier", "parts": ["name"]}}
                        ]
                    }
                },
                "optimized": True,
                "parse_time_ms": 0.45
            }
        }
