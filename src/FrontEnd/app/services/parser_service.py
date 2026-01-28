import sys
import os
import json
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Add the cpp_lib output directory to Python path for the compiled module
_cpp_lib_path = os.path.join(os.path.dirname(__file__), "..", "..", "cpp_lib", "output")
if os.path.exists(_cpp_lib_path):
    sys.path.insert(0, _cpp_lib_path)

try:
    import optisql_parser
    _PARSER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"C++ parser module not available: {e}. Using fallback parser.")
    _PARSER_AVAILABLE = False


@dataclass
class ParseResult:
    success: bool
    ast: Optional[dict] = None
    error_message: Optional[str] = None
    error_line: Optional[int] = None
    error_column: Optional[int] = None
    optimized: bool = False


class ParserService:
    def __init__(self):
        self._parser = None
        if _PARSER_AVAILABLE:
            self._parser = optisql_parser.SQLParser()
            logger.info("C++ SQL parser initialized successfully")
        else:
            logger.warning("C++ SQL parser not available, parse operations will fail")

    @property
    def is_available(self) -> bool:
        return self._parser is not None

    def parse(self, sql: str, optimize: bool = True) -> ParseResult:
        if not self.is_available:
            return ParseResult(
                success=False,
                error_message="C++ parser module not available. Build with 'make python' in cpp_lib directory."
            )

        try:
            json_result = self._parser.parse_to_json(sql, optimize)
            result_dict = json.loads(json_result)

            if result_dict.get("error"):
                return ParseResult(
                    success=False,
                    error_message=result_dict.get("message", "Unknown parse error"),
                    error_line=result_dict.get("line"),
                    error_column=result_dict.get("column")
                )

            return ParseResult(
                success=True,
                ast=result_dict,
                optimized=optimize
            )
        except Exception as e:
            logger.exception(f"Parser error: {e}")
            return ParseResult(
                success=False,
                error_message=str(e)
            )

    def parse_to_dict(self, sql: str, optimize: bool = True) -> dict:
        result = self.parse(sql, optimize)
        if result.success:
            return {
                "success": True,
                "ast": result.ast,
                "optimized": result.optimized
            }
        return {
            "success": False,
            "error": {
                "message": result.error_message,
                "line": result.error_line,
                "column": result.error_column
            }
        }

    def parse_to_json(self, sql: str, optimize: bool = True) -> str:
        return json.dumps(self.parse_to_dict(sql, optimize))


# Singleton instance for convenience
_parser_service: Optional[ParserService] = None


def get_parser_service() -> ParserService:
    global _parser_service
    if _parser_service is None:
        _parser_service = ParserService()
    return _parser_service
