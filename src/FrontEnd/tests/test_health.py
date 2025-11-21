import pytest
from fastapi.testclient import TestClient


def test_health_endpoint(client: TestClient):
    """Test that the health endpoint returns a successful response."""
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "version" in data


def test_health_endpoint_structure(client: TestClient):
    """Test that the health endpoint returns the correct structure."""
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    data = response.json()

    # Check required fields
    assert "status" in data
    assert "version" in data

    # Check data types
    assert isinstance(data["status"], str)
    assert isinstance(data["version"], str)
