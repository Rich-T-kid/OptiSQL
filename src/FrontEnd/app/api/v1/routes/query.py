from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from app.models.schemas import (
    SQLQueryResponse,
    ParseOnlyRequest,
    ParseOnlyResponse,
    ParseResultModel,
    ParseErrorDetail,
    QueryStatus
)
from app.services.parser_service import get_parser_service
from app.services.backend_client import get_backend_client
from app.services.s3_service import get_s3_service
import logging
import time
import uuid
import csv
import io
from typing import Optional, List, Dict, Any

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/parse", response_model=ParseOnlyResponse, tags=["Parser"])
async def parse_sql(request: ParseOnlyRequest):
    """
    Parse a SQL query and return the Abstract Syntax Tree (AST).

    This endpoint only parses the SQL - it does not execute it.
    Useful for validation, debugging, and inspection of query structure.

    Args:
        request: ParseOnlyRequest with sql_query and optimize flag

    Returns:
        ParseOnlyResponse: Parsed AST or error details
    """
    start_time = time.time()
    parser = get_parser_service()

    if not parser.is_available:
        raise HTTPException(
            status_code=503,
            detail="SQL parser not available. Build the C++ parser module with 'make python' in cpp_lib directory."
        )

    result = parser.parse(request.sql_query, request.optimize)
    parse_time = (time.time() - start_time) * 1000

    if result.success:
        return ParseOnlyResponse(
            success=True,
            sql_query=request.sql_query,
            ast=result.ast,
            optimized=result.optimized,
            parse_time_ms=parse_time
        )
    else:
        return ParseOnlyResponse(
            success=False,
            sql_query=request.sql_query,
            error=ParseErrorDetail(
                message=result.error_message or "Unknown error",
                line=result.error_line,
                column=result.error_column
            ),
            parse_time_ms=parse_time
        )


@router.post("/query", response_model=SQLQueryResponse, tags=["Query Processing"])
async def process_sql_query(
    sql_query: str = Form(..., description="SQL query to process"),
    file_uri: Optional[str] = Form(None, description="URI to a remote file (optional if file is provided)"),
    file: Optional[UploadFile] = File(default=None, description="Data file to process (optional if file_uri is provided)"),
    optimize: bool = Form(default=True, description="Whether to apply query optimizations")
):
    """
    Process a SQL query against an uploaded file or a file from a URI.

    The query is first parsed and optimized using the C++ SQL parser,
    then sent to the backend for execution.

    Args:
        sql_query: SQL query string to execute
        file: Uploaded file (CSV, JSON, Parquet, etc.) - optional
        file_uri: URI to a remote file (e.g., s3://bucket/file.csv) - optional
        optimize: Whether to apply query optimizations (default: True)

    Returns:
        SQLQueryResponse: Query processing results including AST and execution result
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

    # Step 1: Parse the SQL query
    parser = get_parser_service()

    if not parser.is_available:
        raise HTTPException(
            status_code=503,
            detail="SQL parser not available. Build the C++ parser module with 'make python' in cpp_lib directory."
        )

    parse_result = parser.parse(sql_query, optimize)

    if not parse_result.success:
        execution_time = (time.time() - start_time) * 1000
        return SQLQueryResponse(
            status=QueryStatus.PARSE_ERROR,
            query=sql_query,
            parse_result=ParseResultModel(
                success=False,
                error=ParseErrorDetail(
                    message=parse_result.error_message or "Unknown parse error",
                    line=parse_result.error_line,
                    column=parse_result.error_column
                )
            ),
            error_message=parse_result.error_message,
            execution_time_ms=execution_time
        )

    # Step 2: Prepare source information
    s3_source = None
    mime_type = "text/csv"  # Default

    if file_uri:
        s3_source = file_uri
        # Determine mime type from URI
        if file_uri.endswith(".json"):
            mime_type = "application/json"
        elif file_uri.endswith(".parquet"):
            mime_type = "application/vnd.apache.parquet"
        elif file_uri.endswith(".csv"):
            mime_type = "text/csv"

    elif file:
        # Validate file extension
        filename = file.filename or "data.csv"
        ext = filename.lower().split('.')[-1] if '.' in filename else ''
        if ext not in ('csv', 'json', 'parquet'):
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: .{ext}. Supported types: .csv, .json, .parquet"
            )

        # Upload file to S3
        s3_service = get_s3_service()
        if not s3_service.is_available:
            raise HTTPException(
                status_code=503,
                detail="S3 service not available. Configure S3_ACCESS_KEY and S3_SECRET_KEY."
            )

        try:
            file_content = await file.read()
            s3_source = await s3_service.upload_file_async(file_content, filename, file.content_type)
            mime_type = s3_service.get_mime_type(filename)
            logger.info(f"Uploaded file to S3: {s3_source}")
        except Exception as e:
            logger.error(f"Failed to upload file to S3: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to upload file to S3: {str(e)}"
            )

    # Step 3: Execute query via backend (if gRPC is available)
    backend = get_backend_client()

    if not backend.is_available:
        # Return parse result only - backend not available
        execution_time = (time.time() - start_time) * 1000
        logger.warning("Backend not available, returning parse result only")
        return SQLQueryResponse(
            status=QueryStatus.SUCCESS,
            query=sql_query,
            parse_result=ParseResultModel(
                success=True,
                ast=parse_result.ast,
                optimized=parse_result.optimized
            ),
            result={
                "message": "Query parsed successfully. Backend execution not available (gRPC not configured).",
                "note": "Run 'make grpc-gen' and ensure backend is running to enable query execution."
            },
            execution_time_ms=execution_time
        )

    try:
        client_id = str(uuid.uuid4())
        query_response = await backend.execute_query(
            sql_statement=sql_query,
            ast=parse_result.ast,
            client_id=client_id,
            s3_source=s3_source,
            mime_type=mime_type
        )

        execution_time = (time.time() - start_time) * 1000

        if query_response.success:
            # Fetch and parse the results from S3
            result_data: Dict[str, Any] = {
                "s3_result_link": query_response.s3_result_link
            }

            if query_response.s3_result_link:
                try:
                    # The result link is just the key (e.g., "SELECT-name-...-uuid")
                    result_key = query_response.s3_result_link
                    s3_service = get_s3_service()

                    # Download the CSV results
                    csv_content = await s3_service.download_file_async(result_key)
                    csv_text = csv_content.decode('utf-8')

                    # Parse CSV into rows
                    reader = csv.DictReader(io.StringIO(csv_text))
                    rows: List[Dict[str, Any]] = list(reader)

                    result_data["columns"] = reader.fieldnames or []
                    result_data["rows"] = rows
                    result_data["row_count"] = len(rows)

                    logger.info(f"Fetched {len(rows)} result rows")
                except Exception as e:
                    logger.warning(f"Failed to fetch results from S3: {e}")
                    result_data["fetch_error"] = str(e)

            return SQLQueryResponse(
                status=QueryStatus.SUCCESS,
                query=sql_query,
                parse_result=ParseResultModel(
                    success=True,
                    ast=parse_result.ast,
                    optimized=parse_result.optimized
                ),
                result=result_data,
                execution_time_ms=execution_time
            )
        else:
            return SQLQueryResponse(
                status=QueryStatus.EXECUTION_ERROR,
                query=sql_query,
                parse_result=ParseResultModel(
                    success=True,
                    ast=parse_result.ast,
                    optimized=parse_result.optimized
                ),
                error_message=query_response.error_message,
                execution_time_ms=execution_time
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        execution_time = (time.time() - start_time) * 1000
        return SQLQueryResponse(
            status=QueryStatus.ERROR,
            query=sql_query,
            parse_result=ParseResultModel(
                success=True,
                ast=parse_result.ast,
                optimized=parse_result.optimized
            ),
            error_message=str(e),
            execution_time_ms=execution_time
        )
