import pytest
from fastapi.testclient import TestClient
import io


def test_query_endpoint_with_file(client: TestClient, sample_csv_file):
    """Test the query endpoint with a file upload."""
    filename, content, content_type = sample_csv_file

    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT * FROM data"},
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["query"] == "SELECT * FROM data"
    assert "result" in data
    assert "execution_time_ms" in data


def test_query_endpoint_with_file_uri(client: TestClient):
    """Test the query endpoint with a file URI."""
    response = client.post(
        "/api/v1/query",
        data={
            "sql_query": "SELECT * FROM data",
            "file_uri": "https://example.com/data.csv"
        }
    )

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "success"
    assert data["query"] == "SELECT * FROM data"
    assert "result" in data
    assert "execution_time_ms" in data


def test_query_endpoint_missing_both_file_and_uri(client: TestClient):
    """Test that the endpoint rejects requests without file or file_uri."""
    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT * FROM data"}
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
            "sql_query": "SELECT * FROM data",
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
        data={"sql_query": "SELECT * FROM data WHERE age > 25"},
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    assert response.status_code == 200
    data = response.json()

    # Check required fields
    assert "status" in data
    assert "query" in data
    assert "result" in data
    assert "execution_time_ms" in data

    # Check data types
    assert isinstance(data["status"], str)
    assert isinstance(data["query"], str)
    assert isinstance(data["execution_time_ms"], (int, float))


def test_query_endpoint_with_json_file(client: TestClient, sample_json_file):
    """Test the query endpoint with a JSON file upload."""
    filename, content, content_type = sample_json_file

    response = client.post(
        "/api/v1/query",
        data={"sql_query": "SELECT * FROM data"},
        files={"file": (filename, io.BytesIO(content), content_type)}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
