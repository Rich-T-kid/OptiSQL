import pytest
from unittest.mock import patch, MagicMock
import json


class TestParseEndpoint:
    """Tests for the /api/v1/parse endpoint."""

    def test_parse_simple_select(self, client):
        """Test parsing a simple SELECT query."""
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": "SELECT name, age FROM users", "optimize": False}
        )

        # If parser not available, expect 503
        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["sql_query"] == "SELECT name, age FROM users"
        assert "ast" in data
        assert data["ast"]["type"] == "SelectStatement"

    def test_parse_with_optimization(self, client):
        """Test parsing with constant folding optimization."""
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": "SELECT 1 + 1, name FROM users", "optimize": True}
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["optimized"] is True

        # Check that 1 + 1 was folded to 2
        first_item = data["ast"]["select"]["items"][0]["expression"]
        assert first_item["type"] == "Literal"
        assert first_item["value"] == 2

    def test_parse_without_optimization(self, client):
        """Test parsing without optimization."""
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": "SELECT 1 + 1 FROM users", "optimize": False}
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["optimized"] is False

        # Check that 1 + 1 was NOT folded (still a BinaryExpr)
        first_item = data["ast"]["select"]["items"][0]["expression"]
        assert first_item["type"] == "BinaryExpr"

    def test_parse_complex_query(self, client):
        """Test parsing a complex query with JOINs and WHERE."""
        sql = """
        SELECT u.name, o.total
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.total > 100
        ORDER BY o.total DESC
        LIMIT 10
        """
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": sql, "optimize": True}
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "from" in data["ast"]
        assert "where" in data["ast"]
        assert "orderBy" in data["ast"]
        assert "limit" in data["ast"]

    def test_parse_error_invalid_sql(self, client):
        """Test parsing invalid SQL returns error."""
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": "SELECT FROM WHERE", "optimize": True}
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "error" in data
        assert data["error"]["message"] is not None

    def test_parse_error_missing_query(self, client):
        """Test that missing sql_query returns validation error."""
        response = client.post(
            "/api/v1/parse",
            json={"optimize": True}
        )
        assert response.status_code == 422

    def test_parse_case_expression(self, client):
        """Test parsing CASE expressions."""
        sql = "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM users"
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": sql, "optimize": False}
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        first_item = data["ast"]["select"]["items"][0]["expression"]
        assert first_item["type"] == "CaseExpr"

    def test_parse_subquery(self, client):
        """Test parsing subqueries."""
        sql = "SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)"
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": sql, "optimize": False}
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["ast"]["where"]["type"] == "InExpr"

    def test_parse_aggregations(self, client):
        """Test parsing aggregate functions."""
        sql = "SELECT COUNT(*), SUM(amount), AVG(price) FROM orders GROUP BY category"
        response = client.post(
            "/api/v1/parse",
            json={"sql_query": sql, "optimize": False}
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "groupBy" in data["ast"]


class TestQueryEndpointWithParser:
    """Tests for the /api/v1/query endpoint with parser integration."""

    def test_query_parse_error(self, client):
        """Test that invalid SQL returns parse error status."""
        response = client.post(
            "/api/v1/query",
            data={
                "sql_query": "SELECT FROM WHERE",
                "file_uri": "s3://bucket/data.csv"
            }
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "parse_error"
        assert data["parse_result"]["success"] is False

    def test_query_success_with_s3_uri(self, client):
        """Test successful query with S3 URI returns AST."""
        response = client.post(
            "/api/v1/query",
            data={
                "sql_query": "SELECT name, age FROM users WHERE age > 25",
                "file_uri": "s3://bucket/data.csv"
            }
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        # Status should be success (or error if backend unavailable, which is fine)
        assert data["status"] in ["success", "error", "execution_error"]
        # Parse result should be successful
        assert data["parse_result"]["success"] is True
        assert data["parse_result"]["ast"]["type"] == "SelectStatement"

    def test_query_with_optimization(self, client):
        """Test query with optimization enabled."""
        response = client.post(
            "/api/v1/query",
            data={
                "sql_query": "SELECT 1 + 1, name FROM users",
                "file_uri": "s3://bucket/data.csv",
                "optimize": "true"
            }
        )

        if response.status_code == 503:
            pytest.skip("C++ parser not available")

        assert response.status_code == 200
        data = response.json()
        assert data["parse_result"]["optimized"] is True
        # 1 + 1 should be folded to 2
        first_item = data["parse_result"]["ast"]["select"]["items"][0]["expression"]
        assert first_item["value"] == 2


class TestParserService:
    """Unit tests for the ParserService class."""

    def test_parser_service_parse_success(self):
        """Test ParserService.parse with valid SQL."""
        from app.services.parser_service import ParserService

        parser = ParserService()
        if not parser.is_available:
            pytest.skip("C++ parser not available")

        result = parser.parse("SELECT a, b FROM t", optimize=False)
        assert result.success is True
        assert result.ast is not None
        assert result.ast["type"] == "SelectStatement"

    def test_parser_service_parse_error(self):
        """Test ParserService.parse with invalid SQL."""
        from app.services.parser_service import ParserService

        parser = ParserService()
        if not parser.is_available:
            pytest.skip("C++ parser not available")

        result = parser.parse("SELECT FROM", optimize=False)
        assert result.success is False
        assert result.error_message is not None

    def test_parser_service_optimization(self):
        """Test ParserService optimization flag."""
        from app.services.parser_service import ParserService

        parser = ParserService()
        if not parser.is_available:
            pytest.skip("C++ parser not available")

        # With optimization
        result_opt = parser.parse("SELECT 1 + 1 FROM t", optimize=True)
        assert result_opt.success is True
        assert result_opt.optimized is True

        # Without optimization
        result_no_opt = parser.parse("SELECT 1 + 1 FROM t", optimize=False)
        assert result_no_opt.success is True
        assert result_no_opt.optimized is False

    def test_parser_service_to_dict(self):
        """Test ParserService.parse_to_dict method."""
        from app.services.parser_service import ParserService

        parser = ParserService()
        if not parser.is_available:
            pytest.skip("C++ parser not available")

        result = parser.parse_to_dict("SELECT 1 FROM t")
        assert isinstance(result, dict)
        assert result["success"] is True
        assert "ast" in result

    def test_parser_service_to_json(self):
        """Test ParserService.parse_to_json method."""
        from app.services.parser_service import ParserService

        parser = ParserService()
        if not parser.is_available:
            pytest.skip("C++ parser not available")

        result = parser.parse_to_json("SELECT 1 FROM t")
        assert isinstance(result, str)
        data = json.loads(result)
        assert data["success"] is True
