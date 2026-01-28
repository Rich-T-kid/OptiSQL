import pytest
from fastapi.testclient import TestClient
import io


def test_query_endpoint_with_file(client: TestClient, sample_csv_file):
    """Test the query endpoint with a file upload."""
    filename, content, content_type = sample_csv_file

    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT name, age FROM data"},
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    # Skip if parser not available
    if response.status_code == 503:
        pytest.skip("C++ parser not available")

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["query"] == "SELECT name, age FROM data"
    assert "result" in data
    assert "execution_time_ms" in data
    # New: check parse_result is present
    assert "parse_result" in data
    assert data["parse_result"]["success"] is True


def test_query_endpoint_with_file_uri(client: TestClient):
    """Test the query endpoint with a file URI."""
    response = client.post(
        "/api/v1/query",
        data={
            "sql_query": "SELECT name, age FROM data",
            "file_uri": "https://example.com/data.csv"
        }
    )

    if response.status_code == 503:
        pytest.skip("C++ parser not available")

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["query"] == "SELECT name, age FROM data"
    assert "result" in data
    assert "execution_time_ms" in data
    assert "parse_result" in data


def test_query_endpoint_missing_both_file_and_uri(client: TestClient):
    """Test that the endpoint rejects requests without file or file_uri."""
    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT name FROM data"}
    )

    assert response.status_code == 400
    data = response.json()
    assert "detail" in data
    assert "file" in data["detail"].lower() or "uri" in data["detail"].lower()


def test_query_endpoint_with_both_file_and_uri(client: TestClient, sample_csv_file):
    """Test that the endpoint rejects requests with both file and file_uri."""
    filename, content, content_type = sample_csv_file

    response = client.post(
        "/api/v1/query",
        data={
            "sql_query": "SELECT name FROM data",
            "file_uri": "https://example.com/data.csv"
        },
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    assert response.status_code == 400
    data = response.json()
    assert "detail" in data


def test_query_endpoint_missing_sql_query(client: TestClient, sample_csv_file):
    """Test that the endpoint requires sql_query parameter."""
    filename, content, content_type = sample_csv_file

    response = client.post(
        "/api/v1/query",
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    assert response.status_code == 422  # Validation error


def test_query_endpoint_response_structure(client: TestClient, sample_csv_file):
    """Test that the query endpoint returns the correct response structure."""
    filename, content, content_type = sample_csv_file

    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT name, age, city FROM data WHERE age > 25"},
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    if response.status_code == 503:
        pytest.skip("C++ parser not available")

    assert response.status_code == 200
    data = response.json()

    # Check required fields
    assert "status" in data
    assert "query" in data
    assert "result" in data
    assert "execution_time_ms" in data
    assert "parse_result" in data

    # Check data types
    assert isinstance(data["status"], str)
    assert isinstance(data["query"], str)
    assert isinstance(data["execution_time_ms"], (int, float))

    # Check parse_result structure
    assert isinstance(data["parse_result"], dict)
    assert "success" in data["parse_result"]
    assert "ast" in data["parse_result"] or "error" in data["parse_result"]


def test_query_endpoint_with_json_file(client: TestClient, sample_json_file):
    """Test the query endpoint with a JSON file upload."""
    filename, content, content_type = sample_json_file

    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT name, age FROM data"},
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    if response.status_code == 503:
        pytest.skip("C++ parser not available")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"


def test_query_endpoint_with_invalid_sql(client: TestClient, sample_csv_file):
    """Test that invalid SQL returns parse_error status."""
    filename, content, content_type = sample_csv_file

    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT FROM WHERE INVALID"},
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    if response.status_code == 503:
        pytest.skip("C++ parser not available")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "parse_error"
    assert data["parse_result"]["success"] is False
    assert "error" in data["parse_result"]


def test_query_endpoint_with_complex_query(client: TestClient):
    """Test the query endpoint with a complex SQL query."""
    response = client.post(
        "/api/v1/query",
        data={
            "sql_query": """
                SELECT u.name, COUNT(o.id) as order_count
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id
                WHERE u.active = TRUE
                GROUP BY u.name
                HAVING COUNT(o.id) > 5
                ORDER BY order_count DESC
                LIMIT 10
            """,
            "file_uri": "s3://bucket/data.csv"
        }
    )

    if response.status_code == 503:
        pytest.skip("C++ parser not available")

    assert response.status_code == 200
    data = response.json()
    assert data["parse_result"]["success"] is True
    ast = data["parse_result"]["ast"]
    assert "from" in ast
    assert "where" in ast
    assert "groupBy" in ast
    assert "having" in ast
    assert "orderBy" in ast
    assert "limit" in ast


def test_query_endpoint_optimization_flag(client: TestClient):
    """Test that the optimize flag affects parsing."""
    # With optimization (default)
    response_opt = client.post(
        "/api/v1/query",
        data={
            "sql_query": "SELECT 1 + 1 FROM data",
            "file_uri": "s3://bucket/data.csv",
            "optimize": "true"
        }
    )

    if response_opt.status_code == 503:
        pytest.skip("C++ parser not available")

    data_opt = response_opt.json()
    assert data_opt["parse_result"]["optimized"] is True

    # Without optimization
    response_no_opt = client.post(
        "/api/v1/query",
        data={
            "sql_query": "SELECT 1 + 1 FROM data",
            "file_uri": "s3://bucket/data.csv",
            "optimize": "false"
        }
    )

    data_no_opt = response_no_opt.json()
    assert data_no_opt["parse_result"]["optimized"] is False
