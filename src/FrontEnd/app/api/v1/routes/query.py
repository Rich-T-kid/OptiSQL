from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from app.models.schemas import SQLQueryResponse
import logging
import time
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/query", response_model=SQLQueryResponse, tags=["Query Processing"])
async def process_sql_query(
    sql_query: str = Form(..., description="SQL query to process"),
    file_uri: Optional[str] = Form(None, description="URI to a remote file (optional if file is provided)"),
    file: Optional[UploadFile] = File(default=None, description="Data file to process (optional if file_uri is provided)")
):
    """
    Process a SQL query against an uploaded file or a file from a URI.

    Args:
        sql_query: SQL query string to execute
        file: Uploaded file (CSV, JSON, Parquet, etc.) - optional
        file_uri: URI to a remote file (e.g., s3://bucket/file.csv, https://example.com/data.csv) - optional

    Returns:
        SQLQueryResponse: Query processing results

    Note:
        Either 'file' or 'file_uri' must be provided, but not both.
    """
    start_time = time.time()

    logger.info(f"Processing SQL query: {sql_query[:100]}...")

    # Normalize inputs: treat empty strings as None
    if file_uri is not None and file_uri.strip() == "":
        file_uri = None

    # Check if file is actually empty (no filename means no file uploaded)
    if file is not None and (not file.filename or file.filename == ""):
        file = None

    # Validate input: must have either file or file_uri
    if file is None and file_uri is None:
        raise HTTPException(
            status_code=400,
            detail="Either 'file' (uploaded file) or 'file_uri' (file URI) must be provided"
        )

    if file is not None and file_uri is not None:
        raise HTTPException(
            status_code=400,
            detail="Cannot provide both 'file' and 'file_uri'. Please provide only one."
        )

    try:

        # TODO: Implement actual SQL query processing logic

        result = {
            "query_length": len(sql_query),
            "message": "Query processing not yet implemented"
        }

        execution_time = (time.time() - start_time) * 1000

        logger.info(f"Query processed successfully in {execution_time:.2f}ms")

        return SQLQueryResponse(
            status="success",
            query=sql_query,
            result=result,
            execution_time_ms=execution_time
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")