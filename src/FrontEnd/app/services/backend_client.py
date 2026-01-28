import os
import logging
import json
import base64
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

from .ast_transformer import transform_ast, ASTTransformError
from .cache_service import get_cache_service

logger = logging.getLogger(__name__)

# Try to import gRPC - it may not be available until grpc-gen is run
try:
    import grpc
    from app.grpc_gen import operation_pb2, operation_pb2_grpc
    _GRPC_AVAILABLE = True
except ImportError as e:
    logger.warning(f"gRPC modules not available: {e}. Run 'make grpc-gen' to generate.")
    _GRPC_AVAILABLE = False


class ReturnType(Enum):
    SUCCESS = 0
    PARSE_ERROR = 1
    EXECUTION_ERROR = 2
    SOURCE_ERROR = 3
    UPLOAD_ERROR = 4
    OUT_OF_MEMORY = 5
    UNKNOWN_ERROR = 6


@dataclass
class QueryResponse:
    success: bool
    s3_result_link: Optional[str] = None
    error_type: Optional[ReturnType] = None
    error_message: Optional[str] = None


class BackendClient:
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        timeout: float = 30.0
    ):
        self.host = host or os.getenv("BACKEND_HOST", "localhost")
        self.port = port or int(os.getenv("BACKEND_PORT", "50051"))
        self.timeout = timeout
        self._channel = None
        self._stub = None

    @property
    def is_available(self) -> bool:
        return _GRPC_AVAILABLE

    def _get_stub(self):
        if not _GRPC_AVAILABLE:
            raise RuntimeError("gRPC not available. Run 'make grpc-gen' to generate proto files.")

        if self._stub is None:
            target = f"{self.host}:{self.port}"
            self._channel = grpc.insecure_channel(target)
            self._stub = operation_pb2_grpc.SSOperationStub(self._channel)
            logger.info(f"Connected to backend at {target}")

        return self._stub

    def close(self):
        if self._channel:
            self._channel.close()
            self._channel = None
            self._stub = None

    async def execute_query(
        self,
        sql_statement: str,
        ast: Dict[str, Any],
        client_id: str,
        s3_source: Optional[str] = None,
        mime_type: str = "text/csv"
    ) -> QueryResponse:
        if not _GRPC_AVAILABLE:
            return QueryResponse(
                success=False,
                error_type=ReturnType.UNKNOWN_ERROR,
                error_message="gRPC not available. Run 'make grpc-gen' to generate proto files."
            )

        try:
            stub = self._get_stub()
            cache = get_cache_service()

            # Check cache for existing Substrait plan
            cached_plan = cache.get_plan(sql_statement, s3_source)
            if cached_plan:
                logger.info("Using cached Substrait plan")
                backend_ir = cached_plan
            else:
                # Transform parser AST to backend IR format
                try:
                    backend_ir = transform_ast(ast, s3_source=s3_source)
                except ASTTransformError as e:
                    logger.error(f"AST transformation failed: {e}")
                    return QueryResponse(
                        success=False,
                        error_type=ReturnType.PARSE_ERROR,
                        error_message=f"AST transformation failed: {e}"
                    )

                # Cache the plan
                cache.set_plan(sql_statement, backend_ir, s3_source)

            # Backend expects base64-encoded JSON string in the logical_plan field
            ir_json = json.dumps(backend_ir)
            logger.info(f"Sending IR to backend: {ir_json[:500]}...")
            logical_plan = base64.b64encode(ir_json.encode('utf-8')).decode('utf-8')

            # Build the request (source info is now embedded in the IR)
            request = operation_pb2.QueryExecutionRequest(
                logical_plan=logical_plan,
                sql_statement=sql_statement,
                id=client_id
            )

            # Execute the query
            response = stub.ExecuteQuery(request, timeout=self.timeout)

            # Check for errors
            if response.error_type and response.error_type.error_type != 0:
                return QueryResponse(
                    success=False,
                    error_type=ReturnType(response.error_type.error_type),
                    error_message=response.error_type.message
                )

            return QueryResponse(
                success=True,
                s3_result_link=response.s3_result_link
            )

        except grpc.RpcError as e:
            logger.error(f"gRPC error: {e}")
            return QueryResponse(
                success=False,
                error_type=ReturnType.UNKNOWN_ERROR,
                error_message=f"gRPC error: {e.details() if hasattr(e, 'details') else str(e)}"
            )
        except Exception as e:
            logger.exception(f"Backend client error: {e}")
            return QueryResponse(
                success=False,
                error_type=ReturnType.UNKNOWN_ERROR,
                error_message=str(e)
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Singleton instance
_backend_client: Optional[BackendClient] = None


def get_backend_client() -> BackendClient:
    global _backend_client
    if _backend_client is None:
        _backend_client = BackendClient()
    return _backend_client
