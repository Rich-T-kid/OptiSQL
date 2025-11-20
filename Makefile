.PHONY: help go-test rust-test go-run rust-run go-lint rust-lint go-fmt rust-fmt frontend-test frontend-run frontend-docker-build frontend-docker-run frontend-docker-down frontend-setup test-all lint-all fmt-all pre-push

# Default target
help:
	@echo "Available targets:"
	@echo "  make go-test       - Run Go tests"
	@echo "  make rust-test     - Run Rust tests"
	@echo "  make go-run        - Run Go application"
	@echo "  make rust-run      - Run Rust application"
	@echo "  make go-lint       - Run Go linter"
	@echo "  make rust-lint     - Run Rust linter and formatter check"
	@echo "  make go-fmt        - Format Go code"
	@echo "  make rust-fmt      - Format Rust code"
	@echo "  make frontend-test - Run Python/Frontend tests"
	@echo "  make frontend-run  - Run Frontend server (without Docker)"
	@echo "  make frontend-setup - Setup Python virtual environment and install dependencies"
	@echo "  make frontend-docker-build - Build Frontend Docker image"
	@echo "  make frontend-docker-run   - Run Frontend using Docker Compose"
	@echo "  make frontend-docker-down  - Stop Frontend Docker containers"
	@echo "  make test-all      - Run all tests (Go + Rust + Frontend)"
	@echo "  make lint-all      - Run all linters (Go + Rust)"
	@echo "  make fmt-all       - Format all code (Go + Rust)"
	@echo "  make pre-push      - Run fmt, lint, and test (use before pushing)"

# Go targets
go-test:
	@echo "Running Go tests..."
	cd src/Backend/opti-sql-go && go test -v ./...

go-test-race:
	@echo "Running Go tests with race detector..."
	cd src/Backend/opti-sql-go && go test -race -v ./...

go-test-coverage:
	@echo "Running Go tests with coverage..."
	cd src/Backend/opti-sql-go && go test -v -coverprofile=coverage.out ./...
	cd src/Backend/opti-sql-go && go tool cover -func=coverage.out
go-run:
	@echo "Running Go application..."
	cd src/Backend/opti-sql-go && go run main.go

go-build:
	@echo "Building Go application..."
	cd src/Backend/opti-sql-go && go build -o opti-sql-go

go-lint:
	@echo "Running Go linter..."
	cd src/Backend/opti-sql-go && golangci-lint run ./...

go-fmt:
	@echo "Formatting Go code..."
	cd src/Backend/opti-sql-go && go fmt ./...

# Rust targets
rust-test:
	@echo "Running Rust tests..."
	cd src/Backend/opti-sql-rs && cargo test --verbose

rust-run:
	@echo "Running Rust application..."
	cd src/Backend/opti-sql-rs && cargo run

rust-build:
	@echo "Building Rust application..."
	cd src/Backend/opti-sql-rs && cargo build --release

rust-lint:
	@echo "Running Rust linter..."
	cd src/Backend/opti-sql-rs && cargo clippy --all-targets --all-features -- -D warnings

rust-fmt:
	@echo "Formatting Rust code..."
	cd src/Backend/opti-sql-rs && cargo fmt

rust-fmt-check:
	@echo "Checking Rust formatting..."
	cd src/Backend/opti-sql-rs && cargo fmt --check

# Frontend targets
frontend-setup:
	@echo "Setting up Python virtual environment..."
	rm -rf src/FrontEnd/venv
	cd src/FrontEnd && python3.12 -m venv --without-pip venv
	@echo "Installing pip..."
	cd src/FrontEnd && . venv/bin/activate && curl -sS https://bootstrap.pypa.io/get-pip.py | python
	@echo "Installing dependencies..."
	cd src/FrontEnd && . venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
	@echo "Frontend setup completed! Activate with: cd src/FrontEnd && source venv/bin/activate"

frontend-test: frontend-setup
	@echo "Running Frontend/Python tests..."
	cd src/FrontEnd && . venv/bin/activate && pytest -m "not integration"

frontend-run:
	@echo "Running Frontend server..."
	cd src/FrontEnd && . venv/bin/activate && python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8005 

frontend-docker-build:
	@echo "Building Frontend Docker image..."
	@if [ ! -f src/FrontEnd/.env ]; then \
		echo "Creating .env file from root .env..."; \
		cp .env src/FrontEnd/.env; \
	fi
	cd src/FrontEnd && docker compose build

frontend-docker-run:
	@echo "Running Frontend with Docker Compose..."
	@if [ ! -f src/FrontEnd/.env ]; then \
		echo "Creating .env file from root .env..."; \
		cp .env src/FrontEnd/.env; \
	fi
	cd src/FrontEnd && docker compose up -d

frontend-docker-down:
	@echo "Stopping Frontend Docker containers..."
	cd src/FrontEnd && docker compose down

# Combined targets
test-all: go-test rust-test frontend-test
	@echo "All tests completed!"

lint-all: go-lint rust-lint
	@echo "All linting completed!"

fmt-all: go-fmt rust-fmt
	@echo "All formatting completed!"

# Pre-push verification
pre-push: fmt-all lint-all test-all
	@echo "All checks passed! Ready to push."

# Clean targets
clean-go:
	@echo "Cleaning Go build artifacts..."
	cd src/Backend/opti-sql-go && go clean
	rm -f src/Backend/opti-sql-go/opti-sql-go
	rm -f src/Backend/opti-sql-go/coverage.out

clean-rust:
	@echo "Cleaning Rust build artifacts..."
	cd src/Backend/opti-sql-rs && cargo clean

clean-all: clean-go clean-rust
	@echo "All build artifacts cleaned!"
