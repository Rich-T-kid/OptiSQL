# Tests

This directory contains tests for the OptiSQL FastAPI server.

## Test Types

### Unit Tests
Unit tests use FastAPI's `TestClient` and don't require a running server. They are fast and can be run anywhere.

- `test_health.py` - Health endpoint tests
- `test_query.py` - Query endpoint tests

### Integration Tests
Integration tests connect to a real running server. They test the full stack including networking, Docker, etc.

- `test_integration.py` - Tests against a running server

## Running Tests

### Run all tests (unit only, no server required)
```bash
pytest -m "not integration"
```

### Run only unit tests
```bash
pytest tests/test_health.py tests/test_query.py
```

### Run integration tests (requires running server)

First, start the server:
```bash
# Using Docker
docker compose up -d

# Or locally
python -m app.main
```

Then run integration tests:
```bash
pytest -m integration
```

### Run all tests (unit + integration)
```bash
# Make sure server is running first!
docker compose up -d

# Run all tests
pytest
```

### Run specific test file
```bash
pytest tests/test_health.py
pytest tests/test_query.py
pytest tests/test_integration.py
```

### Run with verbose output
```bash
pytest -v
```

### Run with coverage
```bash
pytest --cov=app --cov-report=html
```

## Environment Variables

### `TEST_SERVER_URL`
Set this to test against a different server URL (default: `http://localhost:8000`)

```bash
TEST_SERVER_URL=http://localhost:9000 pytest -m integration
```

## Complete Test Workflow

```bash
# 1. Run unit tests (no server needed)
pytest -m "not integration"

# 2. Start the server
docker compose up -d

# 3. Wait for server to be ready
sleep 3

# 4. Run integration tests
pytest -m integration

# 5. Run all tests together
pytest

# 6. Stop the server
docker compose down
```

## Test Structure

- `conftest.py` - Shared fixtures for all tests
- `test_health.py` - Unit tests for the health endpoint
- `test_query.py` - Unit tests for the SQL query processing endpoint
- `test_integration.py` - Integration tests against running server

## Test Fixtures

### `client`
A TestClient instance for making requests to the API without running the server.

### `sample_csv_file`
A sample CSV file fixture for testing file uploads.

### `sample_json_file`
A sample JSON file fixture for testing file uploads.
