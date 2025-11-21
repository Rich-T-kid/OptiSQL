# OptiSQL Frontend API

FastAPI server for SQL query processing and optimization.

## Features

- **Health Check Endpoint**: `/api/v1/health`
- **SQL Query Processing**: `/api/v1/query`
  - Supports file uploads (CSV, JSON, Parquet, Excel)
  - Supports file URIs (HTTP/HTTPS)
  - Configurable logging levels

## Project Structure

```
FrontEnd/
├── app/
│   ├── main.py              # FastAPI application
│   ├── api/v1/routes/       # API endpoints
│   ├── core/                # Config and logging
│   └── models/              # Pydantic schemas
├── tests/                   # Test suite
├── config.yml              # Configuration
├── requirements.txt        # Python dependencies
├── Dockerfile             # Docker image
├── docker-compose.yml     # Docker Compose config
└── pytest.ini            # Pytest configuration
```

## Configuration

Create a `.env` file (or copy from `.env.example`):

```bash
cp .env.example .env
```

Edit `.env` to configure:

```env
# Server Configuration
PORT=8000
HOST=0.0.0.0

# Logging Configuration
# Options: prod, info, debug
LOGGING_MODE=info
```

## Running Locally

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run the Server

```bash
python -m app.main
```

The server will start on the port specified in `config.yml` (default: 8000).

### Access API Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Running with Docker

### Build and Run

```bash
docker compose up --build
```

### Run in Background

```bash
docker compose up -d
```

### View Logs

```bash
docker compose logs -f
```

### Stop the Service

```bash
docker compose down
```

Note: Use `docker compose` (without hyphen) for Docker Compose V2.

## Testing


### Run Unit Tests Only (No Server Required)

```bash
pytest -m "not integration"
```

### Run Integration Tests (Requires Running Server)

```bash
# Start server first
docker compose up -d

# Run integration tests
pytest -m integration
```

### Run All Tests

```bash
pytest
```

### Run with Verbose Output

```bash
pytest -v
```

### Run Specific Tests

```bash
pytest tests/test_health.py
pytest tests/test_query.py
pytest tests/test_integration.py
```

### Test Against Different Server

```bash
TEST_SERVER_URL=http://localhost:9000 pytest -m integration
```

## API Endpoints

### Health Check

```bash
curl http://localhost:8000/api/v1/health
```

### SQL Query Processing (File Upload)

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -F "sql_query=SELECT * FROM data" \
  -F "file=@data.csv"
```

### SQL Query Processing (File URI)

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -F "sql_query=SELECT * FROM data" \
  -F "file_uri=https://example.com/data.csv"
```

## Logging Modes

- **prod**: WARNING level, minimal output
- **info**: INFO level, standard logging
- **debug**: DEBUG level, detailed logs with file/line numbers

## Development

### Hot Reload

When running with Docker Compose, the app directory is mounted as a volume, enabling hot-reload during development.

### Generate Swagger YAML

```bash
python generate_swagger.py
```

This generates `swagger.yml` with the OpenAPI specification.
