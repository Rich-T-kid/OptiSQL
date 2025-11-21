import pytest
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture
def client():
    """
    Create a test client for the FastAPI application.
    This client can be used to make requests to the API without running the server.
    """
    return TestClient(app)


@pytest.fixture
def sample_csv_file():
    """Create a sample CSV file content for testing."""
    content = b"name,age,city\nJohn,30,NYC\nJane,25,LA\nBob,35,Chicago"
    return ("test_data.csv", content, "text/csv")


@pytest.fixture
def sample_json_file():
    """Create a sample JSON file content for testing."""
    content = b'[{"name":"John","age":30,"city":"NYC"},{"name":"Jane","age":25,"city":"LA"}]'
    return ("test_data.json", content, "application/json")
