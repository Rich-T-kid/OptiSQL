import pytest
import httpx
import os
import io


# Get server URL from environment or use default
SERVER_URL = os.getenv("TEST_SERVER_URL", "http://localhost:8000")


@pytest.mark.integration
def test_server_is_running():
    """Test that the server is accessible."""
    try:
        response = httpx.get(f"{SERVER_URL}/api/v1/health", timeout=5.0)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    except httpx.ConnectError:
        pytest.fail(f"Could not connect to server at {SERVER_URL}. Is it running?")


@pytest.mark.integration
def test_health_endpoint_live():
    """Test the health endpoint on a running server."""
    response = httpx.get(f"{SERVER_URL}/api/v1/health", timeout=5.0)

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "healthy"
    assert "version" in data
    assert isinstance(data["version"], str)


@pytest.mark.integration
def test_query_endpoint_with_file_live():
    """Test the query endpoint with file upload on a running server."""
    # Create a test CSV file
    csv_content = b"name,age,city\nJohn,30,NYC\nJane,25,LA"

    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    data = {"sql_query": "SELECT * FROM data"}

    response = httpx.post(
        f"{SERVER_URL}/api/v1/query",
        files=files,
        data=data,
        timeout=10.0
    )

    assert response.status_code == 200
    result = response.json()

    assert result["status"] == "success"
    assert result["query"] == "SELECT * FROM data"
    assert "execution_time_ms" in result


@pytest.mark.integration
def test_query_endpoint_with_uri_live():
    """Test the query endpoint with file URI on a running server."""
    data = {
        "sql_query": "SELECT * FROM data",
        "file_uri": "https://example.com/data.csv"
    }

    response = httpx.post(
        f"{SERVER_URL}/api/v1/query",
        data=data,
        timeout=10.0
    )

    assert response.status_code == 200
    result = response.json()

    assert result["status"] == "success"
    assert result["query"] == "SELECT * FROM data"


@pytest.mark.integration
def test_query_endpoint_validation_live():
    """Test that validation works on a running server."""
    # Missing both file and file_uri
    response = httpx.post(
        f"{SERVER_URL}/api/v1/query",
        data={"sql_query": "SELECT * FROM data"},
        timeout=10.0
    )

    assert response.status_code == 400
    error = response.json()
    assert "detail" in error


@pytest.mark.integration
def test_openapi_docs_accessible():
    """Test that the OpenAPI/Swagger documentation is accessible."""
    response = httpx.get(f"{SERVER_URL}/docs", timeout=5.0)
    assert response.status_code == 200

    response = httpx.get(f"{SERVER_URL}/redoc", timeout=5.0)
    assert response.status_code == 200


@pytest.mark.integration
def test_openapi_schema_available():
    """Test that the OpenAPI schema is available."""
    response = httpx.get(f"{SERVER_URL}/openapi.json", timeout=5.0)
    assert response.status_code == 200

    schema = response.json()
    assert "openapi" in schema
    assert "paths" in schema
    assert "/api/v1/health" in schema["paths"]
    assert "/api/v1/query" in schema["paths"]
